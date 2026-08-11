#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeIdentity {
    LegacyCliB10121,
    BundledB10344,
}

impl RuntimeIdentity {
    pub const fn build(self) -> &'static str {
        match self {
            Self::LegacyCliB10121 => "b10121",
            Self::BundledB10344 => "b10344",
        }
    }

    pub const fn version_line(self) -> &'static str {
        match self {
            Self::LegacyCliB10121 => "version: 10121 (555881ebc)",
            Self::BundledB10344 => "version: 10344 (7a20b417f)",
        }
    }

    pub const fn is_bundled(self) -> bool {
        matches!(self, Self::BundledB10344)
    }

    pub fn supports_manifest(self, manifest: &crate::catalog::Manifest) -> bool {
        let _active_runtime_is_closed = self;
        crate::catalog::is_qualified_gemma4_bundle(manifest)
    }
}
