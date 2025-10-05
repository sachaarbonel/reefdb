fn main() {
    println!("cargo:rerun-if-changed=src/distributed/proto/raft.proto");
    println!("cargo:rerun-if-changed=src/distributed/proto/sql.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&[
            "src/distributed/proto/raft.proto",
            "src/distributed/proto/sql.proto",
        ], &["src/distributed/proto"]) 
        .expect("failed to compile protobufs");
}


