// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_protocol as protocol;

use crate::error::{map_status, ClientError};
use crate::transport::Transport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineResponse {
    Ok,
    NotFound,
}

pub fn read_response(transport: &Transport) -> Result<PipelineResponse, ClientError> {
    let (status, body) = transport.read_frame()?;
    match status {
        protocol::RES_OK => Ok(PipelineResponse::Ok),
        protocol::RES_NOT_FOUND => Ok(PipelineResponse::NotFound),
        _ => {
            map_status(status, &body)?;
            Ok(PipelineResponse::Ok)
        }
    }
}
