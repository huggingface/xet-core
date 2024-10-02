fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(&["proto/common.proto"], &["proto"])?;
    tonic_build::configure().compile(&["proto/infra.proto"], &["proto"])?;
    tonic_build::configure().compile(&["proto/alb.proto"], &["proto"])?;
    Ok(())
}
