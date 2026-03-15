{
  description = "Turbo++ dev shell for anything-sync-daemon-backed R installs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        rWithPackages = pkgs.rWrapper.override {
          packages = with pkgs.rPackages; [
            BiocManager
            jsonlite
          ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            rustc
            clippy
            rustfmt
            ccache
            pkg-config
            cmake
            gnumake
            gcc
            gfortran
            perl
            git
            jq
            openssl
            zlib
            bzip2
            xz
            libdeflate
            libxml2
            curl
            libuv
            icu
            pcre2
            sqlite
            cairo
            pango
            freetype
            harfbuzz
            fribidi
            fontconfig
            glib
            libgit2
            libjpeg
            libpng
            libtiff
            libwebp
            lerc
            rWithPackages
          ];

          shellHook = ''
            export CC="ccache gcc"
            export CXX="ccache g++"
            export FC="gfortran"
            export F77="gfortran"

            export PROJECT_ROOT="$PWD"
            export CARGO_HOME="$PROJECT_ROOT/.cargo-home"
            export RUSTC_WRAPPER="ccache"
            export CCACHE_DIR="$PROJECT_ROOT/.ccache"
            export CCACHE_BASEDIR="$PROJECT_ROOT"
            export CCACHE_COMPRESS=1
            export CCACHE_SLOPPINESS="time_macros"
            export DOWNLOAD_STATIC_LIBV8=1

            export R_ARTIFACT_CACHE="$PROJECT_ROOT/.cache/r-artifacts"
            export MAKEFLAGS="-j4"

            mkdir -p "$CCACHE_DIR" "$CARGO_HOME" "$R_ARTIFACT_CACHE"

            if [ -n "$ASD_DIR" ]; then
              export TURBO_ASD_ROOT="$ASD_DIR/async-r"
            else
              export TURBO_ASD_ROOT="$HOME/.local/share/asd/async-r"
            fi

            export TMPDIR="$TURBO_ASD_ROOT/tmp"
            export R_LIBS_USER="$TURBO_ASD_ROOT/library"

            mkdir -p "$TMPDIR" "$R_LIBS_USER"

            export PKG_CONFIG_PATH="${pkgs.lib.getDev pkgs.openssl}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libxml2}/lib/pkgconfig:${pkgs.lib.getDev pkgs.curl}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libuv}/lib/pkgconfig:${pkgs.lib.getDev pkgs.zlib}/lib/pkgconfig:${pkgs.lib.getDev pkgs.sqlite}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libtiff}/lib/pkgconfig:${pkgs.lib.getDev pkgs.freetype}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libpng}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libjpeg}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libwebp}/lib/pkgconfig:${pkgs.lib.getDev pkgs.lerc}/lib/pkgconfig''${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
            export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.openssl pkgs.curl pkgs.libuv pkgs.zlib pkgs.libxml2 pkgs.sqlite pkgs.freetype pkgs.libpng pkgs.libjpeg pkgs.libtiff pkgs.libwebp pkgs.lerc pkgs.harfbuzz pkgs.fribidi pkgs.fontconfig pkgs.glib pkgs.icu ]}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            export PATH="$PROJECT_ROOT/target/debug:$PATH"
            if [ ! -x "$PROJECT_ROOT/target/debug/du_hast_r" ]; then
              cargo build --bin du_hast_r >/dev/null 2>&1 || true
            fi

            cat <<EOF
turbo++ dev shell ready
anything-sync-daemon root: $TURBO_ASD_ROOT
R library path: $R_LIBS_USER
temporary build dir: $TMPDIR
persistent ccache: $CCACHE_DIR
persistent artifact cache: $R_ARTIFACT_CACHE

Expected host setup:
  anything-sync-daemon should manage $TURBO_ASD_ROOT
  so R installs and temp files stay RAM-backed and sync in the background.
EOF
          '';
        };
      });
}
