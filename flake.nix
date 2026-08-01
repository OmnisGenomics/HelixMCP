{
  description = "HelixMCP BioMCP Fabric Gateway";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f:
        nixpkgs.lib.genAttrs systems (system:
          f (import nixpkgs { inherit system; }));
    in
    {
      packages = forAllSystems (pkgs:
        let
          helixmcp = pkgs.buildNpmPackage {
            pname = "helixmcp-biomcp-fabric";
            version = "0.0.0";
            src = ./.;
            npmDepsHash = "sha256-8bm/btoFo0ifzgrK/fp5mpDt7/tVMVWogJ3pwdEo72k=";
            npmBuildScript = "build";

            installPhase = ''
              runHook preInstall
              mkdir -p "$out/lib/helixmcp"
              cp -R dist package.json package-lock.json "$out/lib/helixmcp/"
              runHook postInstall
            '';
          };
        in
        {
          default = helixmcp;
        });

      checks = forAllSystems (pkgs:
        let
          system = pkgs.stdenv.hostPlatform.system;
          mkNpmCheck = name: script: pkgs.buildNpmPackage {
            pname = "helixmcp-${name}";
            version = "0.0.0";
            src = ./.;
            npmDepsHash = "sha256-8bm/btoFo0ifzgrK/fp5mpDt7/tVMVWogJ3pwdEo72k=";
            dontNpmBuild = true;

            checkPhase = ''
              runHook preCheck
              export TMPDIR=/tmp
              npm run ${script}
              runHook postCheck
            '';
            doCheck = true;

            installPhase = ''
              runHook preInstall
              mkdir -p "$out"
              touch "$out/${name}"
              runHook postInstall
            '';
          };
        in
        {
          default = self.checks.${system}.test;
          package = self.packages.${system}.default;
          typecheck = mkNpmCheck "typecheck" "typecheck";
          test = mkNpmCheck "test" "test";
        });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.nodejs_24
          ];
        };
      });
    };
}
