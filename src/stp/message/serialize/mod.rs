#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::{marker::PhantomData, sync::Arc};
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::message::Header;
use atlas_core::state_transfer::networking::serialize::StateTransferMessage;
use atlas_core::state_transfer::networking::signature_ver::StateTransferVerificationHelper;
use atlas_smr_application::state::divisible_state::DivisibleState;
use serde::{Serialize, Deserialize};

use super::StMessage;

pub struct STMsg<S> (PhantomData<S>);

impl<S: DivisibleState + for<'a> Deserialize<'a> + Serialize + Send + Sync + Clone> StateTransferMessage for STMsg<S>{

    type StateTransferMessage = StMessage<S>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: atlas_capnp::cst_messages_capnp::cst_message::Builder, msg: &Self::StateTransferMessage) -> atlas_common::error::Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: atlas_capnp::cst_messages_capnp::cst_message::Reader) -> atlas_common::error::Result<Self::StateTransferMessage> {
        todo!()
    }

    fn verify_state_message<NI, SVH>(network_info: &Arc<NI>,
                                          header: &Header,
                                          message: Self::StateTransferMessage) -> std::result::Result<StMessage<S>, atlas_common::error::Error>
        where NI: NetworkInformationProvider, SVH: StateTransferVerificationHelper {
            Ok(message)
    }
}

