{ pkgs ? import (fetchTarball {
    # nixos commit at duckdb 0.10.1
    url = "https://github.com/NixOS/nixpkgs/archive/44c64ea5d7d1ce931e616313de11ac171dbd6a40.tar.gz";
  }) {} }:

pkgs.mkShell {
  buildInputs = [pkgs.duckdb.lib pkgs.nix-index];
  
  shellHook = ''
    export DUCKDB_LIB_DIR=${pkgs.duckdb.lib}/lib
    export LD_LIBRARY_PATH=${pkgs.duckdb.lib}/lib:$LD_LIBRARY_PATH
    echo "DuckDB lib at $DUCKDB_LIB_DIR"
    echo "DuckDB shared lib: $DUCKDB_LIB_DIR/libduckdb.so"
  '';
}
