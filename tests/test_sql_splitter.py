"""O splitter e codigo delicado: um bug nele quebra a migration em producao
depois de o CI passar verde. Por isso tem teste proprio."""

from __future__ import annotations

from ledger.db import read_sql, split_sql_statements


def test_keeps_plpgsql_body_intact():
    """Split ingenuo em ';' picaria o corpo da funcao em pedacos invalidos."""
    script = """
    CREATE FUNCTION f() RETURNS TRIGGER AS $fn$
    BEGIN
        SELECT 1; SELECT 2;
        RETURN NULL;
    END;
    $fn$ LANGUAGE plpgsql;
    CREATE TABLE t (id INT);
    """
    statements = split_sql_statements(script)
    assert len(statements) == 2
    assert statements[0].count("SELECT") == 2
    assert statements[0].endswith("LANGUAGE plpgsql")
    assert statements[1].startswith("CREATE TABLE")


def test_ignores_semicolon_in_string_literal():
    statements = split_sql_statements("INSERT INTO t VALUES ('a;b'); SELECT 1;")
    assert len(statements) == 2
    assert "'a;b'" in statements[0]


def test_handles_escaped_quote():
    statements = split_sql_statements("INSERT INTO t VALUES ('it''s; fine'); SELECT 1;")
    assert len(statements) == 2


def test_ignores_semicolon_in_line_comment():
    statements = split_sql_statements("-- comentario; com ponto e virgula\nSELECT 1;")
    assert len(statements) == 1


def test_real_migration_script_parses():
    """O arquivo de verdade, com DO block, constraint trigger e duas funcoes."""
    statements = split_sql_statements(read_sql("0001_up.sql"))
    assert len(statements) > 15
    assert all(s.strip() for s in statements)
    joined = " ".join(statements)
    assert "assert_transaction_balanced" in joined
    assert "DEFERRABLE INITIALLY DEFERRED" in joined
    # os corpos plpgsql sobreviveram inteiros
    bodies = [s for s in statements if "$fn$" in s]
    assert len(bodies) == 2
    assert all(s.count("$fn$") == 2 for s in bodies)
