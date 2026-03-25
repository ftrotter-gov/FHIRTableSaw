"""
Tests for DBTable utility.
"""
import pytest
from src.utils.dbtable import DBTable, DBTableError, DBTableValidationError, DBTableHierarchyError


def test_dbtable_basic_creation():
    """Test basic DBTable creation with schema and table."""
    table = DBTable(schema='public', table='users')
    assert str(table) == 'public.users'
    assert table.schema == 'public'
    assert table.table == 'users'


def test_dbtable_with_database():
    """Test DBTable with database, schema, and table."""
    table = DBTable(database='mydb', schema='public', table='users')
    assert str(table) == 'mydb.public.users'
    assert table.database == 'mydb'
    assert table.schema == 'public'
    assert table.table == 'users'


def test_dbtable_child_creation():
    """Test creating child tables with suffix."""
    parent = DBTable(schema='public', table='practitioners')
    child = parent.make_child('telecom')
    
    assert str(child) == 'public.practitioners_telecom'
    assert child.table == 'practitioners_telecom'
    assert child.schema == 'public'


def test_dbtable_create_child_alias():
    """Test create_child method (alias for make_child)."""
    parent = DBTable(schema='public', table='practitioners')
    child = parent.create_child('addresses')
    
    assert str(child) == 'public.practitioners_addresses'


def test_dbtable_child_strips_leading_underscore():
    """Test that child creation strips leading underscore from suffix."""
    parent = DBTable(schema='public', table='base')
    child = parent.make_child('_suffix')
    
    assert str(child) == 'public.base_suffix'


def test_dbtable_requires_two_levels():
    """Test that DBTable requires at least 2 hierarchy levels."""
    with pytest.raises(DBTableHierarchyError):
        DBTable(table='users')


def test_dbtable_name_validation():
    """Test DBTable name validation rules."""
    # Valid names
    DBTable(schema='my_schema', table='my_table')
    DBTable(schema='schema123', table='table456')
    
    # Invalid: starts with number
    with pytest.raises(DBTableValidationError):
        DBTable(schema='123schema', table='users')
    
    # Invalid: special characters
    with pytest.raises(DBTableValidationError):
        DBTable(schema='my@schema', table='users')
    
    # Invalid: too long (> 60 chars)
    with pytest.raises(DBTableValidationError):
        DBTable(schema='a' * 61, table='users')


def test_dbtable_parameter_aliases():
    """Test that parameter aliases work correctly."""
    # schema_name should work as alias for schema
    table1 = DBTable(schema_name='public', table_name='users')
    assert str(table1) == 'public.users'
    
    # db should work as alias for database
    table2 = DBTable(db='mydb', schema='public', table='users')
    assert str(table2) == 'mydb.public.users'


def test_dbtable_repr():
    """Test DBTable repr for debugging."""
    table = DBTable(schema='public', table='users')
    repr_str = repr(table)
    assert 'DBTable' in repr_str
    assert 'schema' in repr_str
    assert 'table' in repr_str
    assert 'public' in repr_str
    assert 'users' in repr_str


def test_dbtable_cannot_have_table_and_view():
    """Test that cannot specify both table and view."""
    with pytest.raises(DBTableHierarchyError):
        DBTable(schema='public', table='users', view='users_view')
