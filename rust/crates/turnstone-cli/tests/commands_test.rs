// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_cli::commands::parse_command_line;

#[test]
fn parse_exec_line() {
    let (cmd, parts) = parse_command_line("set mykey hello world");
    assert_eq!(cmd, "set");
    assert_eq!(parts[0], "mykey");
    assert_eq!(parts[1], "hello world");
}
