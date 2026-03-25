"""
Integration tests for InLaw utility.

These tests verify that InLaw can discover and run validation tests correctly.
Note: These tests don't require a live database connection.
"""
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, MagicMock
from src.utils.inlaw import InLaw, InlawError


def test_inlaw_abstract_class():
    """Test that InLaw is abstract and requires run() implementation."""
    
    # Should not be able to instantiate InLaw directly
    with pytest.raises(TypeError):
        InLaw()


def test_inlaw_get_classes_from_file():
    """Test that InLaw can discover test classes from a file."""
    
    # Create a temporary file with InLaw test classes
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write("""
from src.utils.inlaw import InLaw

class TestClass1(InLaw):
    title = "Test 1"
    
    @staticmethod
    def run(engine, config=None):
        return True

class TestClass2(InLaw):
    title = "Test 2"
    
    @staticmethod
    def run(engine, config=None):
        return True
""")
        temp_file = f.name
    
    try:
        # Discover classes
        classes = InLaw.get_classes_from_file(temp_file)
        
        # Should find 2 classes
        assert len(classes) == 2
        assert all(issubclass(cls, InLaw) for cls in classes)
        
    finally:
        os.unlink(temp_file)


def test_inlaw_color_helpers():
    """Test ANSI color helper methods."""
    
    green_text = InLaw.ansi_green("PASS")
    assert "\033[92m" in green_text  # Green ANSI code
    assert "PASS" in green_text
    
    red_text = InLaw.ansi_red("FAIL")
    assert "\033[91m" in red_text  # Red ANSI code
    assert "FAIL" in red_text


def test_inlaw_run_all_with_mock_engine():
    """Test InLaw.run_all with a mock engine and test classes."""
    
    # Create a temporary directory with test files
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        
        # Create a test file with InLaw classes
        test_file = tmpdir_path / "test_validation.py"
        test_file.write_text("""
from src.utils.inlaw import InLaw

class PassingTest(InLaw):
    title = "This test passes"
    
    @staticmethod
    def run(engine, config=None):
        return True

class FailingTest(InLaw):
    title = "This test fails"
    
    @staticmethod
    def run(engine, config=None):
        return "This is a failure message"
""")
        
        # Create mock engine
        mock_engine = Mock()
        
        # Run all tests in the directory
        # This should raise InlawError because one test fails
        with pytest.raises(InlawError):
            results = InLaw.run_all(
                engine=mock_engine,
                inlaw_dir=str(tmpdir_path),
                config={'test': 'config'}
            )


def test_inlaw_skip_tests_env_var():
    """Test that SKIP_TESTS environment variable works."""
    
    # Set SKIP_TESTS environment variable
    os.environ['SKIP_TESTS'] = '1'
    
    try:
        mock_engine = Mock()
        
        results = InLaw.run_all(
            engine=mock_engine,
            inlaw_dir='/nonexistent',
            config={}
        )
        
        # Should be skipped
        assert results['skipped'] is True
        assert results['total'] == 0
        
    finally:
        # Clean up
        del os.environ['SKIP_TESTS']


def test_inlaw_concrete_implementation():
    """Test a concrete InLaw implementation."""
    
    class ConcreteTest(InLaw):
        title = "Concrete test"
        
        @staticmethod
        def run(engine, config=None):
            # Simple test that always passes
            if config and config.get('should_pass'):
                return True
            return "Test failed"
    
    # Test with passing config
    mock_engine = Mock()
    result = ConcreteTest.run(mock_engine, config={'should_pass': True})
    assert result is True
    
    # Test with failing config
    result = ConcreteTest.run(mock_engine, config={'should_pass': False})
    assert isinstance(result, str)
    assert "failed" in result.lower()
