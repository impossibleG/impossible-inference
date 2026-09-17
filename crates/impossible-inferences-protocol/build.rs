//! Generates the versioned gRPC contract with a bundled `protoc` binary.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?)
                .join("impossible.inferences.v1.bin"),
        )
        .compile_with_config(
            prost,
            &["proto/impossible/inferences/v1/inference.proto"],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/impossible/inferences/v1/inference.proto");
    Ok(())
}
