{
  description = "Dev shell for async R package fetching and installation";

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
            R
          ];

          shellHook = ''
            export CC="ccache gcc"
            export CXX="ccache g++"
            export FC="gfortran"
            export F77="gfortran"
            export CARGO_HOME="$PWD/.cargo-home"
            export RUSTC_WRAPPER="ccache"
            export CCACHE_DIR="$PWD/.ccache"
            export CCACHE_BASEDIR="$PWD"
            export CCACHE_COMPRESS=1
            export CCACHE_SLOPPINESS="time_macros"
            export DOWNLOAD_STATIC_LIBV8=1
            export MAKEFLAGS="-j4"
            export PKG_CONFIG_PATH="${pkgs.lib.getDev pkgs.openssl}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libxml2}/lib/pkgconfig:${pkgs.lib.getDev pkgs.curl}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libuv}/lib/pkgconfig:${pkgs.lib.getDev pkgs.zlib}/lib/pkgconfig:${pkgs.lib.getDev pkgs.sqlite}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libtiff}/lib/pkgconfig:${pkgs.lib.getDev pkgs.freetype}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libpng}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libjpeg}/lib/pkgconfig:${pkgs.lib.getDev pkgs.libwebp}/lib/pkgconfig:${pkgs.lib.getDev pkgs.lerc}/lib/pkgconfig''${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
            export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.openssl pkgs.curl pkgs.libuv pkgs.zlib pkgs.libxml2 pkgs.sqlite pkgs.freetype pkgs.libpng pkgs.libjpeg pkgs.libtiff pkgs.libwebp pkgs.lerc pkgs.harfbuzz pkgs.fribidi pkgs.fontconfig pkgs.glib pkgs.icu ]}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            mkdir -p "$CCACHE_DIR" "$CARGO_HOME"
            export PATH="$PWD/target/debug:$PATH"
            if [ ! -x "$PWD/target/debug/du_hast_r" ]; then
              cargo build --bin du_hast_r >/dev/null 2>&1 || true
            fi
            echo "dev shell ready"
            echo "R: $(R --version | head -n 1)"
            echo "Rust: $(rustc --version)"
            echo "ccache dir: $CCACHE_DIR"
          '';
        };
      });
}
