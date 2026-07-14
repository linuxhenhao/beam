use crate::workflow_definition::*;

#[test]
fn parse_workflow_definition_accepts_basic_graph() {
    let def = parse_workflow_definition(
        r#"{
            "workflowId":"flow-a",
            "version":1,
            "nodes":{
                "a":{"type":"subagent","bot":"bot-a","prompt":"hi"},
                "b":{"type":"hostExecutor","executor":"feishu-send","input":1,"depends":["a"],"humanGate":{"stage":"before","prompt":"approve?"}}
            }
        }"#,
    )
    .expect("definition");
    assert_eq!(def.workflow_id, "flow-a");
    assert_eq!(def.nodes.len(), 2);
}

// -- Task 1.1: node id validation --

#[test]
fn reject_node_id_with_slash() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"node/a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("nodeId"), "got: {err}");
}

#[test]
fn reject_node_id_with_space() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"node a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("nodeId"), "got: {err}");
}

#[test]
fn reject_node_id_dotdot() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"..":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("nodeId"), "got: {err}");
}

#[test]
fn reject_node_id_containing_dotdot() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a..b":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("nodeId"), "got: {err}");
}

#[test]
fn reject_empty_node_id() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("nodeId"), "got: {err}");
}

#[test]
fn accept_node_id_with_dash() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"node-a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
    )
    .expect("dash ok");
    assert!(def.nodes.contains_key("node-a"));
}

#[test]
fn accept_node_id_with_underscore() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"node_a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
    )
    .expect("underscore ok");
    assert!(def.nodes.contains_key("node_a"));
}

#[test]
fn accept_node_id_with_dot() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"node.a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
    )
    .expect("dot ok");
    assert!(def.nodes.contains_key("node.a"));
}

// -- Task 1.2: side-effect executor gate validation --

#[test]
fn reject_ungated_feishu_send() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":1}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("humanGate") || err.to_string().contains("side-effect"),
        "got: {err}"
    );
}

#[test]
fn reject_ungated_feishu_reply() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-reply","input":1}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("humanGate") || err.to_string().contains("side-effect"),
        "got: {err}"
    );
}

#[test]
fn reject_ungated_beam_schedule() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":1}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("humanGate") || err.to_string().contains("side-effect"),
        "got: {err}"
    );
}

#[test]
fn accept_gated_feishu_send() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":1,"humanGate":{"stage":"before","prompt":"ok?"}}}}"#,
    )
    .expect("gated feishu-send ok");
    assert!(def.nodes.contains_key("a"));
}

#[test]
fn accept_unsafe_allow_ungated_feishu_send() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":1,"unsafeAllowUngated":true}}}"#,
    )
    .expect("unsafeAllowUngated ok");
    assert!(def.nodes.contains_key("a"));
}

#[test]
fn accept_ungated_non_side_effect_executor() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"custom-tool","input":1}}}"#,
    )
    .expect("non-side-effect ok");
    assert!(def.nodes.contains_key("a"));
}

// -- Task 8.2: loop nodes are now accepted (validation deferred to Task 8.3) --

#[test]
fn accept_loop_node_with_minimal_body() {
    // Sink loops need both an output-producing body node and a gated decision.
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"work":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["work"]},"l":{"type":"loop","maxIterations":3,"body":["work","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"work"}}}}"#,
    )
    .expect("loop accepted");
    assert!(def.nodes.contains_key("l"));
}

#[test]
fn reject_standalone_decision_node() {
    // Task 8.3: standalone Decision nodes (not used as a loop terminate.node) are rejected
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("standalone"),
        "expected 'standalone' in error, got: {err}"
    );
}

#[test]
fn ordinary_dag_workflow_accepted() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"subagent","bot":"b","prompt":"p"},"c":{"type":"subagent","bot":"b","prompt":"q","depends":["a"]}}}"#,
    )
    .expect("ordinary DAG ok");
    assert_eq!(def.nodes.len(), 2);
}

// -- Task 8.2: code-review-loop workflow now parses successfully --
#[test]
fn accept_code_review_loop_workflow_json() {
    let raw = include_str!("../../../../workflows/code-review-loop.workflow.json");
    let def = parse_workflow_definition(raw).expect("code-review-loop parsed");
    assert!(def.nodes.contains_key("review-loop"));
    assert!(def.nodes.contains_key("implement"));
    assert!(def.nodes.contains_key("review"));
    assert!(def.nodes.contains_key("reviewDecision"));
}

// -- Task 9.2: subagent-approval-feishu-send example parses --
#[test]
fn accept_subagent_approval_feishu_send_workflow_json() {
    let raw = include_str!("../../../../workflows/subagent-approval-feishu-send.workflow.json");
    let def = parse_workflow_definition(raw).expect("subagent-approval-feishu-send parsed");
    assert_eq!(def.workflow_id, "subagent-approval-feishu-send");
    assert_eq!(def.nodes.len(), 2);
    assert!(def.nodes.contains_key("draft"));
    assert!(def.nodes.contains_key("send"));
    // send depends on draft
    let send_node = def.nodes.get("send").unwrap();
    match send_node {
        // send is a gated feishu-send: humanGate present, unsafeAllowUngated absent
        WorkflowNode::HostExecutor(n) => {
            assert_eq!(n.executor, "feishu-send");
            assert_eq!(
                n.base.depends.as_deref(),
                Some(vec!["draft".to_string()].as_slice())
            );
            assert!(n.base.human_gate.is_some(), "send must have humanGate");
            assert!(
                !n.base.unsafe_allow_ungated.unwrap_or(false),
                "send must NOT use unsafeAllowUngated"
            );
        }
        _ => panic!("expected hostExecutor"),
    }
}

// -- Task 8.3: loop definition validation --

// Rule 1: body nodes must exist
#[test]
fn reject_loop_with_missing_body_node() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":["missing","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("body node") && err.to_string().contains("missing"),
        "expected body node 'missing' error, got: {err}"
    );
}

// Rule 2: body node cannot be a loop (no nested loops)
#[test]
fn reject_loop_with_nested_loop_body() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"loop","maxIterations":2,"body":["d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("nested loop") || err.to_string().contains("cannot be a Loop"),
        "expected nested loop error, got: {err}"
    );
}

// Rule 3a: terminate.node must exist
#[test]
fn reject_loop_with_missing_terminate_node() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"nonexistent","via":"humanGate"},"output":{"from":"d"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("terminate.node") && err.to_string().contains("nonexistent"),
        "expected terminate.node not found error, got: {err}"
    );
}

// Rule 3a: terminate.node must be in loop body
#[test]
fn reject_loop_with_terminate_node_not_in_body() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"x":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"x","via":"humanGate"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("terminate.node") && err.to_string().contains("body"),
        "expected terminate.node must be in body error, got: {err}"
    );
}

// Rule 3a: terminate.node must be a Decision node
#[test]
fn reject_loop_with_non_decision_terminate_node() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["a"],"terminate":{"node":"a","via":"humanGate"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("Decision node"),
        "expected Decision node error, got: {err}"
    );
}

#[test]
fn reject_loop_with_ungated_decision_terminate_node() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"work":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision","depends":["work"]},"l":{"type":"loop","maxIterations":3,"body":["work","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"work"}}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("humanGate"), "got: {err}");
}

#[test]
fn reject_loop_with_non_human_gate_terminate_via() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"work":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["work"]},"l":{"type":"loop","maxIterations":3,"body":["work","d"],"terminate":{"node":"d","via":"automatic"},"output":{"from":"work"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("terminate.via") && err.to_string().contains("humanGate"),
        "got: {err}"
    );
}

// Rule 3b: Decision node cannot be standalone
#[test]
fn reject_decision_not_used_by_any_loop() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision"}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("standalone"),
        "expected standalone Decision error, got: {err}"
    );
}

// Rule 3b: Decision node must be in body of the loop that uses it
#[test]
fn reject_decision_outside_loop_body() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":[],"terminate":{"node":"d","via":"humanGate"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("body"),
        "expected body error for terminate.node not in body, got: {err}"
    );
}

// Rule 3b: same Decision cannot be used by multiple loops
#[test]
fn reject_decision_used_by_multiple_loops() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l1":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}},"l2":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("multiple loops"),
        "expected 'multiple loops' error, got: {err}"
    );
}

// Rule 4: each loop body can have at most one Decision (the terminate.node)
#[test]
fn reject_loop_with_extra_decision_in_body() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d1":{"type":"decision"},"d2":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":["d1","d2"],"terminate":{"node":"d1","via":"humanGate"},"output":{"from":"d1"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("at most one Decision"),
        "expected 'at most one Decision' error, got: {err}"
    );
}

// Rule 5: body external deps must appear in loop.depends
#[test]
fn reject_body_node_with_undeclared_external_dependency() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"ext":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p","depends":["ext"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("external") && err.to_string().contains("depends"),
        "expected external dep must be declared in loop.depends error, got: {err}"
    );
}

#[test]
fn accept_body_node_with_external_dependency_declared_in_loop_depends() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"ext":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner"]},"inner":{"type":"subagent","bot":"b","prompt":"p","depends":["ext"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"depends":["ext"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
    )
    .expect("loop with declared external dep accepted");
    assert!(def.nodes.contains_key("l"));
}

// Rule 6: external nodes cannot depend on loop body nodes
#[test]
fn reject_external_node_depending_on_loop_body_node() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"ext":{"type":"subagent","bot":"b","prompt":"p","depends":["inner"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("depends on loop body node"),
        "expected depends on loop body node error, got: {err}"
    );
}

#[test]
fn accept_external_node_depending_on_loop_node_instead_of_body_node() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner"]},"inner":{"type":"subagent","bot":"b","prompt":"p"},"ext":{"type":"subagent","bot":"b","prompt":"p","depends":["l"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
    )
    .expect("external node depends on loop node accepted");
    assert!(def.nodes.contains_key("l"));
}

// Rule 6 (cont): loop node itself must not depend on body nodes
// (either its own body or another loop's body)
#[test]
fn reject_loop_depends_on_own_body_node() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"depends":["inner"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("body node"),
        "expected 'body node' error for loop depending on own body node, got: {err}"
    );
}

#[test]
fn reject_loop_depends_on_another_loop_body_node() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d1":{"type":"decision"},"a":{"type":"subagent","bot":"b","prompt":"p"},"l1":{"type":"loop","maxIterations":3,"body":["a","d1"],"terminate":{"node":"d1","via":"humanGate"},"output":{"from":"a"}},"d2":{"type":"decision"},"l2":{"type":"loop","maxIterations":3,"body":["d2"],"depends":["a"],"terminate":{"node":"d2","via":"humanGate"},"output":{"from":"d2"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("body node") && err.to_string().contains("'l2'"),
        "expected 'body node' error for l2 depending on another loop's body node, got: {err}"
    );
}

// Also verify that loop depends on another loop (not body node) is still accepted
#[test]
fn accept_loop_depends_on_another_loop_node() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d1":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["a"]},"a":{"type":"subagent","bot":"b","prompt":"p"},"l1":{"type":"loop","maxIterations":3,"body":["a","d1"],"terminate":{"node":"d1","via":"humanGate"},"output":{"from":"a"}},"b":{"type":"subagent","bot":"b","prompt":"q"},"d2":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["b"]},"l2":{"type":"loop","maxIterations":3,"body":["b","d2"],"depends":["l1"],"terminate":{"node":"d2","via":"humanGate"},"output":{"from":"b"}}}}"#,
    )
    .expect("loop depends on another loop node accepted");
    assert!(def.nodes.contains_key("l1"));
    assert!(def.nodes.contains_key("l2"));
}

// Rule 8: sink loop must declare output.from
#[test]
fn reject_sink_loop_without_output_from() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"}}}}"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("output.from"),
        "expected 'output.from' error for sink loop, got: {err}"
    );
}

#[test]
fn accept_sink_loop_with_output_from() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner"]},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
    )
    .expect("sink loop with output.from accepted");
    assert!(def.nodes.contains_key("l"));
}

#[test]
fn reject_sink_loop_with_unknown_output_from() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"inner":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"missing"}}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("loop body"), "got: {err}");
}

#[test]
fn reject_sink_loop_with_external_output_from() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"external":{"type":"subagent","bot":"b","prompt":"p"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"external"}}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("loop body"), "got: {err}");
}

#[test]
fn reject_sink_loop_with_decision_output_from() {
    let err = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"inner":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}}}}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("output-producing"), "got: {err}");
}

#[test]
fn accept_non_sink_loop_without_output_from() {
    // Loop is not a sink because external node 'ext' depends on it
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner"]},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"}},"ext":{"type":"subagent","bot":"b","prompt":"p","depends":["l"]}}}"#,
    )
    .expect("non-sink loop without output.from accepted");
    assert!(def.nodes.contains_key("l"));
}

// Edge case: loop with depends on external node but no body external deps (valid)
#[test]
fn accept_loop_with_explicit_depends_and_no_body_external_deps() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"ext":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner"]},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"depends":["ext"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
    )
    .expect("loop with explicit depends accepted");
    assert!(def.nodes.contains_key("l"));
}

// Regression: multiple loops in same workflow
#[test]
fn accept_two_independent_loops() {
    let def = parse_workflow_definition(
        r#"{"workflowId":"f","version":1,"nodes":{"d1":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner1"]},"inner1":{"type":"subagent","bot":"b","prompt":"p"},"l1":{"type":"loop","maxIterations":3,"body":["inner1","d1"],"terminate":{"node":"d1","via":"humanGate"},"output":{"from":"inner1"}},"d2":{"type":"decision","humanGate":{"stage":"approve","prompt":"continue?"},"depends":["inner2"]},"inner2":{"type":"subagent","bot":"b","prompt":"p"},"l2":{"type":"loop","maxIterations":3,"body":["inner2","d2"],"depends":["l1"],"terminate":{"node":"d2","via":"humanGate"},"output":{"from":"inner2"}}}}"#,
    )
    .expect("two independent loops accepted");
    assert!(def.nodes.contains_key("l1"));
    assert!(def.nodes.contains_key("l2"));
}
