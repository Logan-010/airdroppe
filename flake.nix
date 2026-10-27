{
  description = "Airdroppe Flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
      naersk,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        lib = pkgs.lib;
        isLinux = pkgs.stdenv.hostPlatform.isLinux;

        toolchain = fenix.packages.${system}.latest.toolchain;

        naersk-lib = pkgs.callPackage naersk {
          cargo = toolchain;
          rustc = toolchain;
        };

        frontend = pkgs.buildNpmPackage {
          pname = "airdroppe-frontend";
          version = "0.1.0";

          src = lib.cleanSourceWith {
            src = ./ui;
            filter =
              path: type:
              lib.cleanSourceFilter path type
              && !(builtins.elem (builtins.baseNameOf path) [
                "node_modules"
                "dist"
              ]);
          };

          nodejs = pkgs.nodejs_26;
          npmDepsHash = "sha256-yrlMGBHIGhlkYodY8QeyXXxskcbF86mFLBdEPJQjoLM=";

          installPhase = ''
            runHook preInstall
            mkdir -p "$out"
            cp -r dist/. "$out/"
            runHook postInstall
          '';
        };

        nativeBuildInputs = with pkgs; [
          pkg-config
        ];

        buildInputs =
          with pkgs;
          [
            openssl
            stdenv.cc.cc.lib
          ]
          ++ lib.optionals isLinux ([
            gtk3
            cairo
            glib
            dbus
            gdk-pixbuf
            webkitgtk_4_1
            libsoup_3
            librsvg
            gsettings-desktop-schemas
          ]);

        runtimeLibraryPath = lib.makeLibraryPath buildInputs;

        app = naersk-lib.buildPackage {
          name = "airdroppe";

          version = (builtins.fromTOML (builtins.readFile ./app/Cargo.toml)).package.version;

          root = ./app;

          src = lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              lib.cleanSourceFilter path type
              && !(builtins.elem (builtins.baseNameOf path) [
                "target"
                "node_modules"
                "dist"
                ".direnv"
              ]);
          };

          inherit nativeBuildInputs buildInputs;

          cargoBuildOptions =
            options:
            options
            ++ [
              "--package"
              "airdroppe"
              "--locked"
            ];

          overrideMain = attrs: {
            nativeBuildInputs =
              attrs.nativeBuildInputs
              ++ lib.optionals isLinux [
                pkgs.wrapGAppsHook3
              ];

            postPatch = (attrs.postPatch or "") + ''
              mkdir -p frontend/dist
              cp -r ${frontend}/. frontend/dist/
            '';

            preFixup =
              (attrs.preFixup or "")
              + lib.optionalString isLinux ''
                gappsWrapperArgs+=(
                  --prefix LD_LIBRARY_PATH : "${runtimeLibraryPath}"
                  --prefix GIO_EXTRA_MODULES : "${pkgs.glib-networking}/lib/gio/modules"
                )
              '';
          };

          meta = {
            description = "Easy P2P file sharing";
            mainProgram = "airdroppe";
            platforms = lib.platforms.linux ++ lib.platforms.darwin;
          };
        };
      in
      {
        packages.default = app;
        packages.airdroppe = app;
        packages.frontend = frontend;

        apps.default = {
          type = "app";
          program = "${app}/bin/airdroppe";
        };

        devShells.default = pkgs.mkShell (
          {
            inherit nativeBuildInputs buildInputs;

            packages = with pkgs; [
              toolchain
              rust-analyzer
              cargo-tauri

              nodejs_26
              typescript
              typescript-language-server
              prefetch-npm-deps

              git
              just

              file
              binutils
            ];

            RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";
          }
          // lib.optionalAttrs isLinux {
            LD_LIBRARY_PATH = runtimeLibraryPath;
            GIO_EXTRA_MODULES = "${pkgs.glib-networking}/lib/gio/modules";
          }
        );
      }
    );
}
