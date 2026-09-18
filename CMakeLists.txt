cmake_minimum_required(VERSION 3.16)
project(fcitx5-voicetype VERSION 0.2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

find_package(ECM 1.0 REQUIRED)
set(CMAKE_MODULE_PATH "${ECM_MODULE_PATH}")
find_package(Fcitx5Core REQUIRED)
find_package(ALSA REQUIRED)
find_package(Threads REQUIRED)

# Rust core (microphone + streaming ASR), built by cargo as a static library.
set(CARGO_TARGET_DIR "${CMAKE_BINARY_DIR}/cargo")
set(RUST_CORE "${CARGO_TARGET_DIR}/release/libvoice_type_core.a")
file(GLOB_RECURSE RUST_SOURCES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.rs")
add_custom_command(
    OUTPUT "${RUST_CORE}"
    COMMAND ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
            cargo build --release --manifest-path "${CMAKE_SOURCE_DIR}/Cargo.toml"
    DEPENDS ${RUST_SOURCES} "${CMAKE_SOURCE_DIR}/Cargo.toml" "${CMAKE_SOURCE_DIR}/Cargo.lock"
    COMMENT "cargo build --release (voice_type_core)"
    VERBATIM)
add_custom_target(voice_type_core DEPENDS "${RUST_CORE}")

add_library(voicetype MODULE fcitx5/voicetype.cpp)
add_dependencies(voicetype voice_type_core)
target_link_libraries(voicetype PRIVATE Fcitx5::Core "${RUST_CORE}"
                      ALSA::ALSA Threads::Threads ${CMAKE_DL_LIBS} m)
target_link_options(voicetype PRIVATE -Wl,--exclude-libs,ALL)

install(TARGETS voicetype DESTINATION "${FCITX_INSTALL_ADDONDIR}")
install(FILES fcitx5/voicetype.conf DESTINATION "${FCITX_INSTALL_PKGDATADIR}/addon")
