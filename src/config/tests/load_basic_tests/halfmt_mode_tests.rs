use super::*;

#[test]
fn halfmt_absent_means_upstream_behavior() {
    let cfg = load_config_from_temp_toml("[general]\n");
    assert!(cfg.halfmt.mode.is_none());
    assert!(!cfg.halfmt.is_client());
    assert!(!cfg.halfmt.is_server());
    assert!(cfg.halfmt.validate().is_ok());
}

#[test]
fn halfmt_client_mode_parses_and_validates() {
    let cfg = load_config_from_temp_toml(
        r#"
        [general]
        [halfmt]
        mode = "client"
        tunnel = "127.0.0.1:11234"
        server = "127.0.0.1:25051"
        "#,
    );
    assert!(cfg.halfmt.is_client());
    assert!(!cfg.halfmt.is_server());
    assert!(cfg.halfmt.validate().is_ok());
    assert_eq!(
        cfg.halfmt.client_tunnel_addr().unwrap().to_string(),
        "127.0.0.1:11234"
    );
    assert_eq!(
        cfg.halfmt.client_server_addr().unwrap().to_string(),
        "127.0.0.1:25051"
    );
}

#[test]
fn halfmt_server_mode_parses_and_validates() {
    let cfg = load_config_from_temp_toml(
        r#"
        [general]
        [halfmt]
        mode = "server"
        listen = "127.0.0.1:25051"
        "#,
    );
    assert!(cfg.halfmt.is_server());
    assert!(cfg.halfmt.validate().is_ok());
    assert_eq!(
        cfg.halfmt.listen_addr().unwrap().to_string(),
        "127.0.0.1:25051"
    );
}

#[test]
fn halfmt_client_mode_requires_addresses() {
    let cfg = load_config_from_temp_toml(
        r#"
        [general]
        [halfmt]
        mode = "client"
        "#,
    );
    let err = cfg.halfmt.validate().unwrap_err();
    assert!(err.contains("halfmt.tunnel"), "unexpected error: {err}");

    let cfg = load_config_from_temp_toml(
        r#"
        [general]
        [halfmt]
        mode = "client"
        tunnel = "127.0.0.1:11234"
        server = "not-an-addr"
        "#,
    );
    let err = cfg.halfmt.validate().unwrap_err();
    assert!(err.contains("halfmt.server"), "unexpected error: {err}");
}
