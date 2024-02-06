use anyhow::anyhow;
use atlas_common::channel::{self, ChannelSyncTx};
use atlas_common::crypto::hash::Digest;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::persistentdb::KVDB;
use atlas_common::threadpool::{self, ThreadPool};
use atlas_core::ordering_protocol::networking::serialize::NetworkView;
use atlas_core::ordering_protocol::ExecutionResult;
use atlas_core::state_transfer::networking::StateTransferSendNode;
use konst::string::split;
use scoped_threadpool::Pool;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use atlas_smr_application::state::divisible_state::{
    DivisibleState, DivisibleStateDescriptor, InstallStateMessage, PartId, StatePart,
};
use log::{debug, info};
use serde::{Deserialize, Serialize};

use crate::stp::message::MessageKind;
use crate::stp::metrics::{
    CHECKPOINT_UPDATE_TIME_ID, PROCESS_REQ_STATE_TIME_ID,
    STATE_TRANSFER_STATE_INSTALL_CLONE_TIME_ID, STATE_TRANSFER_TIME_ID, TOTAL_STATE_INSTALLED_ID,
    TOTAL_STATE_TRANSFERED_ID, TOTAL_STATE_WAIT_ID,
};
use atlas_common::collections::{self, ConcurrentHashMap, HashMap};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_core::persistent_log::{DivisibleStateLog, PersistableStateTransferProtocol};
use atlas_core::state_transfer::divisible_state::*;
use atlas_core::state_transfer::{CstM, STResult, STTimeoutResult, StateTransferProtocol};
use atlas_core::timeouts::{RqTimeout, TimeoutKind, Timeouts};
use atlas_metrics::metrics::{
    metric_duration, metric_duration_end, metric_duration_start, metric_increment,
    metric_store_count,
};

use self::message::serialize::STMsg;
use self::message::StMessage;

pub mod message;
pub mod metrics;

const INSTALL_ITERATIONS: usize = 10;

const STATE: &'static str = "state";

// Split a slice into n slices, modified from https://users.rust-lang.org/t/how-to-split-a-slice-into-n-chunks/40008/5
fn split_evenly<T>(slice: &[T], n: usize) -> impl Iterator<Item = &[T]> {
    struct Iter<'a, I> {
        pub slice: &'a [I],
        pub n: usize,
    }
    impl<'a, I> Iterator for Iter<'a, I> {
        type Item = &'a [I];
        fn next(&mut self) -> Option<&'a [I]> {
            if self.slice.len() == 0 {
                return None;
            }

            if self.n == 0 {
                return Some(self.slice);
            }

            let (first, rest) = self.slice.split_at(self.slice.len() / self.n);
            self.slice = rest;
            self.n -= 1;
            Some(first)
        }
    }

    Iter { slice, n }
}

pub struct StateTransferConfig {
    pub timeout_duration: Duration,
}

struct PersistentCheckpoint<S: DivisibleState> {
    seqno: Mutex<SeqNo>,
    descriptor: RwLock<Option<S::StateDescriptor>>,
    targets: Mutex<Vec<NodeId>>,
    // used to track the state part's position in the persistent checkpoint
    // K = pageId V= Length
    parts: KVDB,
    // list of parts we need in order to recover the state
    req_parts: ConcurrentHashMap<Arc<S::PartDescription>, ()>,
}

impl<S: DivisibleState> Debug for PersistentCheckpoint<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentCheckpoint")
            .field("seqno", &self.seqno)
            .field("descriptor", &self.descriptor)
            .field("targets", &self.targets)
            .field("req_parts", &self.req_parts)
            .finish()
    }
}

impl<S: DivisibleState> Default for PersistentCheckpoint<S> {
    fn default() -> Self {
        Self {
            seqno: Mutex::new(SeqNo::ZERO),
            descriptor: RwLock::new(None),
            // parts: Default::default(),
            req_parts: ConcurrentHashMap::default(),
            targets: Mutex::new(vec![]),
            parts: KVDB::new("checkpoint", vec![STATE]).unwrap(),
        }
    }
}

impl<S: DivisibleState> PersistentCheckpoint<S> {
    pub fn new(id: u32) -> Self {
        let path = format!("{}{}", "checkpoint_", id);
        Self {
            req_parts: ConcurrentHashMap::default(),
            seqno: SeqNo::ZERO.into(),
            descriptor: RwLock::new(None),
            targets: Mutex::new(vec![]),
            parts: KVDB::new(path, vec![STATE]).unwrap(),
        }
    }

    pub fn descriptor(&self) -> Option<S::StateDescriptor> {
        self.descriptor.read().expect("failed to read").clone()
    }

    pub fn descriptor_parts(&self) -> Vec<Arc<S::PartDescription>> {
        let desc = self.descriptor.read().unwrap();
        match desc.as_ref() {
            Some(d) => d.parts().iter().cloned().collect::<Vec<_>>(),
            None => vec![],
        }
    }

    pub fn compare_descriptor(&self, other: &Option<S::StateDescriptor>) -> bool {
        self.descriptor
            .read()
            .expect("failed to read descriptor")
            .eq(other)
    }

    pub fn get_seqno(&self) -> SeqNo {
        self.seqno.lock().expect("failed to acquire lock").clone()
    }

    pub fn update_seqno(&self, seq: SeqNo) {
        let mut lock = self.seqno.lock().expect("failed to update seqno");
        *lock = seq;
    }

    fn read_local_part(&self, part_id: &[u8]) -> Result<Option<S::StatePart>> {
        let key: Vec<u8> = bincode::serialize(part_id).expect("failed to serialize");

        let res = self.parts.get(STATE, key)?;

        match res {
            Some(buf) => {
                let res =
                    bincode::deserialize::<S::StatePart>(&buf).expect("failed to deserialize part");

                Ok(Some(res))
            }
            None => Ok(None),
        }
    }

    fn write_parts(&self, parts: Box<[S::StatePart]>) -> Result<()> {
        let batch = parts.iter().map(|part| {
            (
                bincode::serialize(part.id()).expect("failed to serialize"),
                bincode::serialize(part).unwrap(),
            )
        });

        let _ = self.parts.set_all(STATE, batch);

        Ok(())
    }

    pub fn get_parts(&self, parts_desc: &[S::PartDescription], pool: &mut Pool) -> Result<Box<[S::StatePart]>> {
        // need to figure out what to do if the part read doesn't match the descriptor
        let vec = Arc::new(Mutex::new(Vec::new()));

        let batch = parts_desc.iter().map(|part| {
            (
                STATE,
                bincode::serialize(part.id()).expect("failed to serialize"),
            )
        });

        let binding = self.parts.get_all(batch).expect("failed to get all parts");
        let parts = split_evenly(&binding, 4);

        pool.scoped(|scope| {
            parts.for_each(|chunk| {
                let vec_handle = vec.clone();
                scope.execute(move || {
                    let mut local_vec = Vec::new();
                    for part in chunk {
                        let state_part = match part.as_ref().expect("invalid part") {
                            Some(buf) => {
                                let res = bincode::deserialize::<S::StatePart>(&buf)
                                    .expect("failed to deserialize part");
                                res
                            }
                            None => continue,
                        };

                        local_vec.push(state_part);
                    }

                    vec_handle.lock().expect("failed to lock").extend(local_vec.into_iter());
                });
            });
        });

        let unwrapped_vec = Arc::try_unwrap(vec).expect("Lock still has multiple owners");

        Ok(unwrapped_vec.into_inner().expect("failed to extract vec from mutex").into_boxed_slice())
    }

    pub fn get_parts_by_ref(
        &self,
        parts_desc: &[Arc<S::PartDescription>],
    ) -> Result<(Vec<S::StatePart>, u64)> {
        // need to figure out what to do if the part read doesn't match the descriptor

        let mut vec = Vec::new();
        let mut size = 0;
        let batch = parts_desc.iter().map(|part| {
            (
                STATE,
                bincode::serialize(part.id()).expect("failed to serialize"),
            )
        });
        let parts = self.parts.get_all(batch).expect("failed to get all parts");

        for part in parts {
            let state_part = match part.expect("invalid part") {
                Some(buf) => {
                    let res = bincode::deserialize::<S::StatePart>(&buf)
                        .expect("failed to deserialize part");

                    size = size + res.size() as u64;
                    res
                    /*  if self.contains_part(res.descriptor()).is_some() {
                        res
                    } else {
                        continue;
                    }*/
                }
                None => continue,
            };

            // println!("{:?}", self.contains_part(state_part.descriptor()));
            vec.push(state_part);
        }

        Ok((vec, size))
    }

    pub fn get_req_parts(&self, pool: &mut Pool) {
        let desc_parts = self.descriptor_parts();
        let split = split_evenly(&desc_parts, 4);

        pool.scoped(|scope| {
            split.for_each(|chunk| {
                scope.execute(|| {
                    for part in chunk.iter() {
                        if let Some(local_part) = self
                            .read_local_part(part.id())
                            .expect("failed to read part")
                        {
                            if part.content_description() == local_part.hash().as_ref() {
                                // We've confirmed that this part is valid so we don't need to request it

                                continue;
                            }
                        }

                        self.req_parts.insert(part.clone(), ());
                    }
                });
            });
        });
    }

    pub fn requested_part(&self, part: &S::PartDescription) -> bool {
        self.req_parts.contains_key(part)
    }
    pub fn contains_part(
        &self,
        part: &S::PartDescription,
    ) -> Option<Arc<<S as DivisibleState>::PartDescription>> {
        self.descriptor()
            .unwrap()
            .parts()
            .iter()
            .find(|p| p.as_ref() == part)
            .cloned()
    }

    pub fn update_descriptor(&self, descriptor: Option<S::StateDescriptor>) {
        let mut desc = self.descriptor.write().expect("failed to get lock");
        *desc = descriptor;
    }
}

enum ProtoPhase<S: DivisibleState> {
    Init,
    WaitingCheckpoint(Vec<StoredMessage<StMessage<S>>>),
    ReceivingCid(usize),
    ReceivingStateDescriptor(usize),
    ReceivingState(usize),
}

impl<S: DivisibleState> Debug for ProtoPhase<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtoPhase::Init => {
                write!(f, "Init Phase")
            }
            ProtoPhase::WaitingCheckpoint(header) => {
                write!(f, "Waiting for checkpoint {}", header.len())
            }
            ProtoPhase::ReceivingCid(size) => {
                write!(f, "Receiving CID phase {} responses", size)
            }
            ProtoPhase::ReceivingStateDescriptor(i) => {
                write!(f, "Receiving state descriptor {}", i)
            }
            ProtoPhase::ReceivingState(i) => {
                write!(f, "Receiving state phase responses {}", i)
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RecoveryState<S: DivisibleState> {
    seq: SeqNo,
    pub st_frag: Box<[S::StatePart]>,
}

enum StStatus<S: DivisibleState> {
    Nil,
    Running,
    ReqLatestCid,
    SeqNo(SeqNo),
    RequestStateDescriptor,
    StateDescriptor(S::StateDescriptor),
    ReqState,
    StateComplete(SeqNo),
}

impl<S: DivisibleState> Debug for StStatus<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StStatus::Nil => {
                write!(f, "Nil")
            }
            StStatus::Running => {
                write!(f, "Running")
            }
            StStatus::ReqLatestCid => {
                write!(f, "Request latest CID")
            }
            StStatus::ReqState => {
                write!(f, "Request latest state")
            }
            StStatus::SeqNo(seq) => {
                write!(f, "Received seq no {:?}", seq)
            }
            StStatus::StateComplete(seq) => {
                write!(
                    f,
                    "All parts have been received, can install state {:?}",
                    seq
                )
            }
            StStatus::RequestStateDescriptor => write!(f, "Request State Descriptor"),
            StStatus::StateDescriptor(_) => write!(f, "Received State Descriptor"),
        }
    }
}

// NOTE: in this module, we may use cid interchangeably with
// consensus sequence number
pub struct BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static,
{
    curr_seq: SeqNo,
    checkpoint: Arc<PersistentCheckpoint<S>>,
    largest_cid: SeqNo,
    base_timeout: Duration,
    curr_timeout: Duration,
    timeouts: Timeouts,
    new_descriptor: Option<S::StateDescriptor>,
    node: Arc<NT>,
    phase: ProtoPhase<S>,
    threadpool: Pool,
    received_state_ids: BTreeMap<(SeqNo, Digest), Vec<NodeId>>,
    // received_state_descriptor: HashMap<SeqNo, S::StateDescriptor>,
    install_channel: ChannelSyncTx<InstallStateMessage<S>>,

    /// Persistent logging for the state transfer protocol.
    persistent_log: PL,
}

pub enum StProgress<S: DivisibleState> {
    // TODO: Timeout( some type here)
    /// This value represents null progress in the CST code's state machine.
    Nil,
    /// We have a fresh new message to feed the CST state machine, from
    /// the communication layer.
    Message(Header, StMessage<S>),
}

macro_rules! getmessage {
    ($progress:expr, $status:expr) => {
        match $progress {
            StProgress::Nil => return $status,
            StProgress::Message(h, m) => (h, m),
        }
    };
    // message queued while waiting for exec layer to deliver app state
    ($phase:expr) => {{
        let phase = std::mem::replace($phase, ProtoPhase::Init);
        match phase {
            ProtoPhase::WaitingCheckpoint(h, m) => (h, m),
            _ => return StStatus::Nil,
        }
    }};
}

impl<S, NT, PL> StateTransferProtocol<S, NT, PL> for BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static + Clone + for<'a> Deserialize<'a> + Serialize,
    PL: DivisibleStateLog<S> + 'static,
    NT: StateTransferSendNode<STMsg<S>> + 'static,
{
    type Serialization = STMsg<S>;

    fn request_latest_state<V>(&mut self, view: V) -> Result<()>
    where
        V: NetworkView,
    {
        self.request_latest_consensus_seq_no::<V>(view);

        Ok(())
    }

    fn handle_off_ctx_message<V>(
        &mut self,
        view: V,
        message: StoredMessage<StMessage<S>>,
    ) -> Result<()>
    where
        V: NetworkView,
    {
        let (header, st_message) = message.into_inner();
        let kind = st_message.kind().clone();

        println!(
            "{:?} // Off context Message {:?} from {:?} with seq {:?}",
            self.node.id(),
            st_message,
            header.from(),
            st_message.sequence_number()
        );

        match kind {
            MessageKind::RequestLatestSeq => {
                self.process_request_seq(header, st_message);

                return Ok(());
            }
            MessageKind::RequestStateDescriptor => {
                self.process_request_descriptor(header, st_message);

                return Ok(());
            }
            MessageKind::ReqState(_) => {
                self.process_request_state(header, st_message);

                return Ok(());
            }
            _ => {}
        }

        let status = self.process_message_inner(view, StProgress::Message(header, st_message));

        match status {
            StStatus::Nil => (),
            _ => {
                return Err(anyhow!(
                    "Invalid state reached while state transfer processing message! {:?}",
                    status
                ))
            }
        }
        Ok(())
    }

    fn process_message<V>(
        &mut self,
        view: V,
        message: StoredMessage<StMessage<S>>,
    ) -> Result<STResult>
    where
        V: NetworkView,
    {
        let (header, message) = message.into_inner();

        println!(
            "{:?} // Message {:?} from {:?} while in phase {:?}",
            self.node.id(),
            message,
            header.from(),
            self.phase
        );

        match message.kind() {
            MessageKind::RequestLatestSeq => {
                self.process_request_seq(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            MessageKind::RequestStateDescriptor => {
                self.process_request_descriptor(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            MessageKind::ReqState(_) => {
                self.process_request_state(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            _ => {}
        }

        // Notify timeouts that we have received this message
        self.timeouts
            .received_cst_request(header.from(), message.sequence_number());

        let status = self.process_message_inner(view.clone(), StProgress::Message(header, message));

        match status {
            StStatus::Nil => (),
            StStatus::Running => (),
            StStatus::ReqLatestCid => self.request_latest_consensus_seq_no(view),
            StStatus::SeqNo(seq) => {
                return if self.checkpoint.get_seqno() < seq {
                    self.curr_seq = seq;
                    info!(
                        "{:?} // Current State descriptor {:?}, Requesting state descriptor {:?}",
                        self.node.id(),
                        self.checkpoint.get_seqno(),
                        seq
                    );
                    metric_duration_start(STATE_TRANSFER_TIME_ID);
                    self.request_state_descriptor(view);
                    Ok(STResult::StateTransferRunning)
                } else {
                    info!("{:?} // Not installing sequence number nor requesting state ???? {:?} {:?}", self.node.id(), self.checkpoint.seqno, seq);
                    Ok(STResult::StateTransferNotNeeded(seq))
                }
            }
            StStatus::ReqState => {
                metric_store_count(TOTAL_STATE_TRANSFERED_ID, 0);

                self.request_latest_state_parts(view)?;
            }
            StStatus::RequestStateDescriptor => self.request_state_descriptor(view),
            StStatus::StateDescriptor(descriptor) => {
                metric_duration_start(TOTAL_STATE_WAIT_ID);
                if self
                    .checkpoint
                    .compare_descriptor(&Some(descriptor.clone()))
                {
                    self.checkpoint.update_seqno(self.largest_cid);
                    println!("descriptor is the same, no ST needed");
                    return Ok(STResult::StateTransferNotNeeded(
                        self.checkpoint.get_seqno(),
                    ));
                } else {
                    self.checkpoint.update_descriptor(Some(descriptor));

                    self.checkpoint.get_req_parts(&mut self.threadpool);

                    if self.checkpoint.req_parts.is_empty() {
                        return self.install_state();
                    }

                    self.request_latest_state_parts(view)?;
                }
            }
            StStatus::StateComplete(_seq) => {
                metric_duration_end(TOTAL_STATE_WAIT_ID);
                return self.install_state();
            }
        }

        Ok(STResult::StateTransferRunning)
    }

    fn handle_app_state_requested(&mut self, _seq: SeqNo) -> Result<ExecutionResult> {
        // if self.checkpoint.get_seqno() < seq
        //    || (seq != SeqNo::ZERO && self.checkpoint.descriptor.is_none())
        // {
        metric_duration_start(CHECKPOINT_UPDATE_TIME_ID);

        Ok(ExecutionResult::BeginCheckpoint)
        //  } else {
        //     Ok(ExecutionResult::Nil)
        // }
    }

    fn handle_timeout<V>(&mut self, view: V, timeout: Vec<RqTimeout>) -> Result<STTimeoutResult>
    where
        V: NetworkView,
    {
        for cst_seq in timeout {
            if let TimeoutKind::Cst(cst_seq) = cst_seq.timeout_kind() {
                if self.cst_request_timed_out(cst_seq.clone(), view.clone()) {
                    return Ok(STTimeoutResult::RunCst);
                }
            }
        }

        Ok(STTimeoutResult::CstNotNeeded)
    }

    fn poll(
        &mut self,
    ) -> Result<atlas_core::state_transfer::STPollResult<CstM<Self::Serialization>>> {
        Ok(atlas_core::state_transfer::STPollResult::ReceiveMsg)
    }
}

// TODO: request timeouts
impl<S, NT, PL> BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static + Clone + for<'a> Deserialize<'a> + Serialize,
    PL: DivisibleStateLog<S> + 'static,
    NT: StateTransferSendNode<STMsg<S>> + 'static,
{
    /// Create a new instance of `BtStateTransfer`.
    pub fn new(
        node: Arc<NT>,
        base_timeout: Duration,
        timeouts: Timeouts,
        persistent_log: PL,
        install_channel: ChannelSyncTx<InstallStateMessage<S>>,
    ) -> Self {
        let id = node.id().0;
        let tp = Pool::new(6);
        Self {
            base_timeout,
            curr_timeout: base_timeout,
            timeouts,
            node,
            phase: ProtoPhase::Init,
            largest_cid: SeqNo::ZERO,
            curr_seq: SeqNo::ZERO,
            persistent_log,
            install_channel,
            checkpoint: Arc::new(PersistentCheckpoint::new(id)),
            //received_state_descriptors: HashMap::default(),
            received_state_ids: BTreeMap::default(),
            new_descriptor: None,
            threadpool: tp,
        }
    }

    /// Checks if the CST layer is waiting for a local checkpoint to
    /// complete.
    ///
    /// This is used when a node is sending state to a peer.
    pub fn needs_checkpoint(&self) -> bool {
        matches!(self.phase, ProtoPhase::WaitingCheckpoint(_))
    }

    /// Handle a timeout received from the timeouts layer.
    /// Returns a bool to signify if we must move to the Retrieving state
    /// If the timeout is no longer relevant, returns false (Can remain in current phase)
    pub fn cst_request_timed_out<V>(&mut self, seq: SeqNo, view: V) -> bool
    where
        V: NetworkView,
    {
        let status = self.timed_out(seq);

        match status {
            StStatus::ReqLatestCid => {
                self.request_latest_consensus_seq_no(view);

                true
            }
            StStatus::ReqState => {
                let _ = self.request_latest_state(view);

                true
            }
            // nothing to do
            _ => false,
        }
    }

    fn timed_out(&mut self, seq: SeqNo) -> StStatus<S> {
        if seq != self.curr_seq {
            // the timeout we received is for a request
            // that has already completed, therefore we ignore it
            //
            // TODO: this check is probably not necessary,
            // as we have likely already updated the `ProtoPhase`
            // to reflect the fact we are no longer receiving state
            // from peer nodes
            return StStatus::Nil;
        }

        self.next_seq();

        match self.phase {
            // retry requests if receiving state and we have timed out
            ProtoPhase::ReceivingCid(_) => {
                self.curr_timeout *= 2;
                StStatus::ReqLatestCid
            }
            ProtoPhase::ReceivingState(_) => {
                self.curr_timeout *= 2;
                StStatus::ReqState
            }
            // ignore timeouts if not receiving any kind
            // of state from peer nodes
            _ => StStatus::Nil,
        }
    }

    fn process_request_seq(&mut self, header: Header, message: StMessage<S>) {
        let seq = match self.checkpoint.descriptor() {
            Some(descriptor) => Some((
                self.checkpoint.get_seqno(),
                descriptor.get_digest().unwrap(),
            )),
            None => {
                // We have received no state updates from the app so we have no descriptor
                Some((SeqNo::ZERO, Digest::blank()))
            }
        };

        let kind = MessageKind::ReplyLatestSeq(seq);

        let reply = StMessage::new(message.sequence_number(), kind);

        info!(
            "{:?} // Replying to {:?} seq {:?} with seq no {:?}",
            self.node.id(),
            header.from(),
            message.sequence_number(),
            seq
        );

        let _ = self.node.send(reply, header.from(), true);
    }

    pub fn request_state_descriptor<V>(&mut self, view: V)
    where
        V: NetworkView,
    {
        self.next_seq();

        let cst_seq = self.curr_seq;

        //info!("{:?} // Requesting latest state with cst msg seq {:?}", self.node.id(), cst_seq);

        self.timeouts
            .timeout_cst_request(self.curr_timeout, 1, cst_seq);

        self.phase = ProtoPhase::ReceivingStateDescriptor(0);

        //TODO: Maybe attempt to use followers to rebuild state and avoid
        // Overloading the replicas
        let message = StMessage::new(cst_seq, MessageKind::RequestStateDescriptor);
        let targets = view
            .quorum_members()
            .clone()
            .into_iter()
            .filter(|id| *id != self.node.id());

        let _ = self.node.broadcast(message, targets);
    }
    /// Process the entire list of pending state transfer requests
    /// This will only reply to the latest request sent by each of the replicas
    fn process_pending_state_requests(&mut self) {
        let waiting = std::mem::replace(&mut self.phase, ProtoPhase::Init);

        if let ProtoPhase::WaitingCheckpoint(reqs) = waiting {
            let mut map: HashMap<NodeId, StoredMessage<StMessage<S>>> = collections::hash_map();

            for request in reqs {
                // We only want to reply to the most recent requests from each of the nodes
                if map.contains_key(&request.header().from()) {
                    map.entry(request.header().from()).and_modify(|x| {
                        if x.message().sequence_number() < request.message().sequence_number() {
                            //Dispose of the previous request
                            let _ = std::mem::replace(x, request);
                        }
                    });

                    continue;
                } else {
                    map.insert(request.header().from(), request);
                }
            }

            map.into_values().for_each(|req| {
                let (header, message) = req.into_inner();

                self.process_request_state(header, message);
            });
        }
    }

    fn process_request_state(&mut self, header: Header, message: StMessage<S>) {
        let start = Instant::now();
        match &mut self.phase {
            ProtoPhase::Init => {}
            ProtoPhase::WaitingCheckpoint(waiting) => {
                waiting.push(StoredMessage::new(header, message));

                return;
            }
            _ => {
                // We can't reply to state requests when requesting state ourselves
                return;
            }
        }

        let _ = match self.checkpoint.descriptor() {
            Some(state) => state,
            _ => {
                if let ProtoPhase::WaitingCheckpoint(waiting) = &mut self.phase {
                    waiting.push(StoredMessage::new(header, message));
                } else {
                    self.phase =
                        ProtoPhase::WaitingCheckpoint(vec![StoredMessage::new(header, message)]);
                }

                return;
            }
        }
        .clone();

        let st_frag = match message.kind() {
            MessageKind::ReqState(req_parts) => {
                let parts = req_parts.iter().as_slice();
                self.checkpoint.get_parts(parts, &mut self.threadpool).unwrap()
            }
            _ => {
                return;
            }
        };

        let reply = StMessage::new(
            message.sequence_number(),
            MessageKind::ReplyState(RecoveryState {
                seq: self.checkpoint.get_seqno(),
                st_frag,
            }),
        );

        self.node.send(reply, header.from(), true).unwrap();
        metric_duration(PROCESS_REQ_STATE_TIME_ID, start.elapsed());
        println!("process req state finished {:?}", start.elapsed());
    }

    pub fn process_request_descriptor(&mut self, header: Header, message: StMessage<S>) {
        match &mut self.phase {
            ProtoPhase::Init => {}
            ProtoPhase::WaitingCheckpoint(waiting) => {
                waiting.push(StoredMessage::new(header, message));

                return;
            }
            _ => {
                // We can't reply to state requests when requesting state ourselves
                return;
            }
        }

        let state = match self.checkpoint.descriptor() {
            Some(state) => state,
            _ => {
                if let ProtoPhase::WaitingCheckpoint(waiting) = &mut self.phase {
                    waiting.push(StoredMessage::new(header, message));
                } else {
                    self.phase =
                        ProtoPhase::WaitingCheckpoint(vec![StoredMessage::new(header, message)]);
                }

                return;
            }
        };

        let reply = StMessage::new(
            message.sequence_number(),
            MessageKind::ReplyStateDescriptor(Some((self.checkpoint.get_seqno(), state.clone()))),
        );

        self.node.send(reply, header.from(), true).unwrap();
    }

    fn process_message_inner<V>(&mut self, view: V, progress: StProgress<S>) -> StStatus<S>
    where
        V: NetworkView,
    {
        match self.phase {
            ProtoPhase::WaitingCheckpoint(_) => {
                self.process_pending_state_requests();

                StStatus::Nil
            }
            ProtoPhase::Init => {
                let (header, message) = getmessage!(progress, StStatus::Nil);

                match message.kind() {
                    MessageKind::RequestLatestSeq => {
                        self.process_request_seq(header, message);
                    }
                    MessageKind::RequestStateDescriptor => {
                        self.process_request_descriptor(header, message)
                    }
                    MessageKind::ReqState(_) => {
                        self.process_request_state(header, message);
                    }
                    // we are not running cst, so drop any reply msgs
                    //
                    // TODO: maybe inspect cid msgs, and passively start
                    // the state transfer protocol, by returning
                    // CstStatus::RequestState
                    _ => (),
                }

                StStatus::Nil
            }
            ProtoPhase::ReceivingCid(i) => {
                let (header, message) = getmessage!(progress, StStatus::ReqLatestCid);
                // debug!("{:?} // Received Cid with {} responses from {:?} for CST Seq {:?} vs Ours {:?}", self.node.id(),
                //    i, header.from(), message.sequence_number(), self.curr_seq);

                // drop cst messages with invalid seq no
                if message.sequence_number() != self.curr_seq {
                    //  debug!(
                    //      "{:?} // Wait what? {:?} {:?}",
                    //      self.node.id(),
                    //      self.curr_seq,
                    //      message.sequence_number()
                    //  );
                    // FIXME: how to handle old or newer messages?
                    // BFT-SMaRt simply ignores messages with a
                    // value of `queryID` different from the current
                    // `queryID` a replica is tracking...
                    // we will do the same for now
                    //
                    // TODO: implement timeouts to fix cases like this
                    return StStatus::Running;
                }

                match message.kind() {
                    MessageKind::RequestLatestSeq => {
                        self.process_request_seq(header, message);

                        return StStatus::Running;
                    }
                    MessageKind::ReplyLatestSeq(state_cid) => {
                        if let Some(state_cid) = state_cid {
                            let received_state_from = self
                                .received_state_ids
                                .entry(*state_cid)
                                .or_insert_with(|| vec![header.from()]);

                            if !received_state_from.contains(&header.from()) {
                                // We received the same state from a different node
                                received_state_from.push(header.from());
                            }
                        }
                    }
                    MessageKind::ReqState(_) => {
                        self.process_request_state(header, message);

                        return StStatus::Running;
                    }
                    _ => return StStatus::Running,
                }

                // check if we have gathered enough cid
                // replies from peer nodes

                let i = i + 1;
                let (largest, voters) = self.received_state_ids.last_key_value().unwrap();

                debug!(
                    "{:?} // Quorum count {}, i: {}, cst_seq {:?}. Current Latest Cid: {:?}",
                    self.node.id(),
                    view.quorum(),
                    i,
                    self.curr_seq,
                    self.largest_cid
                );

                if i == view.quorum() && voters.len() >= i {
                    self.phase = ProtoPhase::Init;

                    self.largest_cid = largest.0.clone();

                    // reset timeout, since req was successful
                    self.curr_timeout = self.base_timeout;
                    println!("largest cid {:?}", self.largest_cid);
                    //  info!("{:?} // Identified the latest state seq no as {:?} with {} votes.",
                    //          self.node.id(),
                    //          self.largest_cid, self.latest_cid_count);

                    // we don't need the latest cid to be available in at least
                    // f+1 replicas since the replica has the proof that the system
                    // has decided
                    StStatus::SeqNo(self.largest_cid)
                } else {
                    self.phase = ProtoPhase::ReceivingCid(i);

                    StStatus::Running
                }
            }
            ProtoPhase::ReceivingStateDescriptor(i) => {
                let (header, mut message) = getmessage!(progress, StStatus::RequestStateDescriptor);
                let descriptor = match message.take_descriptor() {
                    Some(descriptor) => descriptor,
                    None => return StStatus::Running,
                };

                let desc_digest = descriptor.1.get_digest().unwrap();

                if let Some(voters) = self
                    .received_state_ids
                    .get_mut(&(descriptor.0, desc_digest))
                {
                    if !voters.contains(&header.from()) {
                        voters.push(header.from());
                    }

                    if voters.len() >= view.quorum() {
                        let mut lock = self.checkpoint.targets.lock().unwrap();
                        *lock = voters.clone().into();
                        self.curr_timeout = self.base_timeout;
                        self.phase = ProtoPhase::Init;
                        self.received_state_ids.clear();
                        println!(
                            "decided on descriptor {:?} {:?}",
                            descriptor.0,
                            descriptor.1.get_digest()
                        );
                        return StStatus::StateDescriptor(descriptor.1.clone());
                    }
                }
                /*  match self
                    .received_state_descriptors
                    .insert(descriptor.0, descriptor.1)
                {
                    Some(prev_descriptor) => {
                        error!("{:?} // Received descriptor {:?} with different digest {:?} should be {:?}", self.node.id(), prev_descriptor.sequence_number(),&desc_digest,prev_descriptor.get_digest());
                    }
                    None => {}
                }*/

                let i = i + 1;

                if i >= view.quorum() {
                    self.phase = ProtoPhase::Init;

                    StStatus::ReqLatestCid
                } else {
                    self.phase = ProtoPhase::ReceivingStateDescriptor(i);

                    StStatus::Running
                }
            }
            ProtoPhase::ReceivingState(i) => {
                let (header, mut message) = getmessage!(progress, StStatus::ReqState);

                if message.sequence_number() != self.curr_seq {
                    // NOTE: check comment above, on ProtoPhase::ReceivingCid
                    return StStatus::Running;
                }

                let state = match message.take_state() {
                    Some(state) => state,
                    // drop invalid message kinds
                    None => return StStatus::Running,
                };

                let frags = split_evenly(&state.st_frag, 4);

                self.threadpool.scoped(|scope| {
                   // let time = Instant::now();

                    frags.for_each(|frag| {
                        scope.execute(|| {

                            let checkpoint_handle = self.checkpoint.clone();
                            let mut accepted_parts = Vec::new();
                            let mut accepted_descriptor = Vec::new();

                            frag.iter().for_each(|received_part| {
                                metric_increment(
                                    TOTAL_STATE_TRANSFERED_ID,
                                    Some(received_part.size()),
                                );
                                if received_part.hash().as_ref()
                                    == received_part.descriptor().content_description()
                                    && checkpoint_handle.requested_part(received_part.descriptor())
                                {
                                    accepted_descriptor.push(received_part.descriptor().clone());
                                    accepted_parts.push(received_part.clone());
                                }
                            });

                            let _ =
                                checkpoint_handle.write_parts(accepted_parts.into_boxed_slice());

                            if !checkpoint_handle.req_parts.is_empty() {
                                //remove the parts that we accepted
                                checkpoint_handle
                                    .req_parts
                                    .retain(|part, _| !accepted_descriptor.contains(part));
                            }
                        });
                    });

                /*     println!(
                        "time to validate fragment {:?} number of parts {:?}",
                        time.elapsed(),
                        state.st_frag.len()
                    );*/
                });

                

                let i = i + 1;

                self.curr_timeout = self.base_timeout;
                let mut targets = self.checkpoint.targets.lock().unwrap();
                if i == targets.len() {
                    self.phase = ProtoPhase::Init;

                    targets.clear();

                    return if self.checkpoint.req_parts.is_empty() {
                        println!("state transfer complete seq: {:?}", state.seq);
                        StStatus::StateComplete(state.seq)
                    } else {
                        // We need to clear the descriptor in order to revert the state of the State transfer protocol to ReqLatestCid,
                        // where we assume our state is wrong, therefore out descriptor is wrong
                        self.checkpoint.update_descriptor(None);
                        StStatus::ReqLatestCid
                    };
                }

                self.phase = ProtoPhase::ReceivingState(i);

                StStatus::Running
            }
        }
    }

    pub fn request_latest_consensus_seq_no<V>(&mut self, view: V)
    where
        V: NetworkView,
    {
        // reset state of latest seq no. request
        self.largest_cid = SeqNo::ZERO;
        self.received_state_ids.clear();

        self.next_seq();

        let cst_seq = self.curr_seq;

        //info!("{:?} // Requesting latest state seq no with seq {:?}", self.node.id(), cst_seq);

        self.timeouts
            .timeout_cst_request(self.curr_timeout, view.quorum() as u32, cst_seq);

        self.phase = ProtoPhase::ReceivingCid(0);

        let message = StMessage::new(cst_seq, MessageKind::RequestLatestSeq);

        let targets = view
            .quorum_members()
            .clone()
            .into_iter()
            .filter(|id| *id != self.node.id());

        let _ = self.node.broadcast(message, targets);
    }

    fn request_latest_state_parts<V>(&mut self, view: V) -> Result<()>
    where
        V: NetworkView,
    {
        let cst_seq = self.curr_seq;

        info!(
            "{:?} // Requesting latest state with cst msg seq {:?}",
            self.node.id(),
            cst_seq
        );

        self.timeouts
            .timeout_cst_request(self.curr_timeout, view.quorum() as u32, cst_seq);

        self.phase = ProtoPhase::ReceivingState(0);

        //TODO: Maybe attempt to use followers to rebuild state and avoid
        // Overloading the replicas

        let targets = self.checkpoint.targets.lock().unwrap();
        let parts_list = self
            .checkpoint
            .req_parts
            .iter()
            .map(|part| part.key().clone())
            .collect::<Box<_>>();
        let parts_map = split_evenly(&parts_list, targets.len()).map(|r| {
            r.into_iter()
                .map(|part| part.as_ref().clone())
                .collect::<Vec<_>>()
        });

        for (p, n) in parts_map.zip(targets.iter()) {
            let message = StMessage::new(cst_seq, MessageKind::ReqState(p.into_boxed_slice()));

            self.node.send(message, *n, false)?;
        }

        Ok(())
    }

    fn next_seq(&mut self) -> SeqNo {
        self.curr_seq = self.curr_seq.next();

        self.curr_seq
    }

    fn install_state(&mut self) -> Result<STResult> {
        println!("START INSTALL STATE");
        metric_store_count(TOTAL_STATE_INSTALLED_ID, 0);

        if self.checkpoint.descriptor().is_none() {
            panic!("No descriptor while installing state");
        }

        // divide the state in parts, useful if the state is too large to keep in memory

        //descriptor.parts()

        let start_install = Instant::now();

        let parts = self.checkpoint.descriptor_parts();
        let state_frags = split_evenly(&parts, INSTALL_ITERATIONS);

        self.threadpool.scoped(|scope| {
            state_frags.for_each(|frag| {
                scope.execute(|| {
                    let (st_frag, size) = self.checkpoint.get_parts_by_ref(frag).unwrap();

                    metric_increment(TOTAL_STATE_INSTALLED_ID, Some(size));

                    self.install_channel
                        .send(InstallStateMessage::StatePart(MaybeVec::from_many(st_frag)))
                        .unwrap();
                });
            });
        });

        self.install_channel
            .send(InstallStateMessage::Done)
            .unwrap();

        self.checkpoint.update_seqno(self.largest_cid);
        self.received_state_ids.clear();

        metric_duration(
            STATE_TRANSFER_STATE_INSTALL_CLONE_TIME_ID,
            start_install.elapsed(),
        );
        metric_duration_end(STATE_TRANSFER_TIME_ID);

        println!(
            "state transfer finished {:?} {:?}",
            self.checkpoint.get_seqno(),
            start_install.elapsed()
        );

        Ok(STResult::StateTransferFinished(self.checkpoint.get_seqno()))
    }
}

impl<S, NT, PL> PersistableStateTransferProtocol for BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static + Send + Clone,
    PL: DivisibleStateLog<S> + 'static,
{
}

impl<S, NT, PL> DivisibleStateTransfer<S, NT, PL> for BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static + Send + Sync + Clone + for<'a> Deserialize<'a> + Serialize,
    PL: DivisibleStateLog<S> + 'static,
    NT: StateTransferSendNode<STMsg<S>> + 'static + Send + Sync,
{
    type Config = StateTransferConfig;

    fn initialize(
        config: Self::Config,
        timeouts: Timeouts,
        node: Arc<NT>,
        log: PL,
        executor_state_handle: ChannelSyncTx<InstallStateMessage<S>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let StateTransferConfig { timeout_duration } = config;
        Ok(Self::new(
            node,
            timeout_duration,
            timeouts,
            log,
            executor_state_handle,
        ))
    }

    fn handle_state_desc_received_from_app(
        &mut self,
        descriptor: <S as DivisibleState>::StateDescriptor,
        seq_no: SeqNo,
    ) -> Result<()> {
        info!(
            "receiving checkpoint {:?} {:?}",
            self.checkpoint.get_seqno(),
            seq_no
        );
        println!(
            "receiving checkpoint {:?} {:?}",
            self.checkpoint.get_seqno(),
            seq_no
        );

        if self.checkpoint.get_seqno() < seq_no {
            info!("receiving checkpoint {:?}", descriptor.get_digest());
            println!("receiving checkpoint {:?}", descriptor.get_digest());

            self.new_descriptor = Some(descriptor);
        }
        Ok(())
    }

    fn handle_state_part_received_from_app(
        &mut self,
        parts: Vec<<S as DivisibleState>::StatePart>,
    ) -> Result<()> {
        if !parts.is_empty() {
            self.checkpoint.write_parts(parts.into_boxed_slice())?;
        }
        Ok(())
    }

    fn handle_state_finished_reception(&mut self, seq_no: SeqNo) -> Result<()> {
        if let Some(desc) = self.new_descriptor.take() {
            info!(
                "{:?} // new checkpoint desc{:?}, seqno {:?}",
                self.node.id(),
                &desc.get_digest(),
                seq_no.next()
            );
            self.checkpoint.update_seqno(seq_no);
            self.checkpoint.update_descriptor(Some(desc));
        }

        metric_duration_end(CHECKPOINT_UPDATE_TIME_ID);

        Ok(())
    }
}
