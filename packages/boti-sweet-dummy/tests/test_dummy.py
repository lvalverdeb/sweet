from boti_sweet_dummy import NAME, describe


def test_describe_reports_installed() -> None:
    assert "installed" in describe()


def test_name_matches_entry_point_name() -> None:
    assert NAME == "dummy"
