def test_foundation_packages_importable() -> None:
    import boti
    import boti_dask
    import boti_data

    assert boti
    assert boti_data
    assert boti_dask
