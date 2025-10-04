def test_imports():
    """Ensure all core modules import successfully."""
    import main
    import etl.extract.parser
    import etl.transform.processor
    import etl.transform.enrichments
    import etl.load.storage
    import etl.load.batch_control
