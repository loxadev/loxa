// swift-tools-version: 6.3

import PackageDescription

let package = Package(
    name: "loxa-mlx",
    platforms: [
        .macOS(.v14),
    ],
    products: [
        .executable(name: "loxa-mlx", targets: ["LoxaMLXCommand"]),
        .library(name: "LoxaMLXCore", targets: ["LoxaMLXCore"]),
        .library(name: "LoxaMLXServer", targets: ["LoxaMLXServer"]),
    ],
    dependencies: [
        .package(
            url: "https://github.com/ml-explore/mlx-swift.git",
            exact: "0.31.6"
        ),
        .package(
            url: "https://github.com/ml-explore/mlx-swift-lm.git",
            exact: "3.31.4"
        ),
        .package(
            url: "https://github.com/huggingface/swift-huggingface.git",
            exact: "0.9.0"
        ),
        .package(
            url: "https://github.com/huggingface/swift-transformers.git",
            exact: "1.3.0"
        ),
    ],
    targets: [
        .target(
            name: "LoxaMLXCore",
            dependencies: [
                .product(name: "MLX", package: "mlx-swift"),
                .product(name: "MLXLLM", package: "mlx-swift-lm"),
                .product(name: "MLXLMCommon", package: "mlx-swift-lm"),
                .product(name: "MLXHuggingFace", package: "mlx-swift-lm"),
                .product(name: "HuggingFace", package: "swift-huggingface"),
                .product(name: "Tokenizers", package: "swift-transformers"),
            ]
        ),
        .target(
            name: "LoxaMLXServer",
            dependencies: ["LoxaMLXCore"]
        ),
        .executableTarget(
            name: "LoxaMLXCommand",
            dependencies: ["LoxaMLXCore", "LoxaMLXServer"]
        ),
        .testTarget(
            name: "LoxaMLXCoreTests",
            dependencies: ["LoxaMLXCore"]
        ),
        .testTarget(
            name: "LoxaMLXServerTests",
            dependencies: ["LoxaMLXServer", "LoxaMLXCore"]
        ),
    ],
    swiftLanguageModes: [.v6]
)
