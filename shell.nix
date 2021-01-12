{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  # deps, pipenv does the heavy lifting
  inputsFrom = with pkgs; [ pipenv brave ];
  buildInputs = with pkgs; [ python39 ];

  # startup pipenv
  shellHook = ''
    ${pkgs.pipenv}/bin/pipenv install
    ${pkgs.pipenv}/bin/pipenv shell
  '';

  # environment
  OPEN = "${pkgs.brave}/bin/brave";
  PORT = "9090";
  SEGMENT_PATH = "/tmp";
  LOG_PATH = "/tmp/commitlog.dat";
}
