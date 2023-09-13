import configs
import pytest
from ..configs.toml_config_loader import MissingConfigDataError

@pytest.mark.parametrize("err_msg", ["Filler error message"])
def test_missing_config_data():
    with pytest.raises(MissingConfigDataError) as exc_info:
        configs.ConfigLoader.get_config_data()

