import pytest
import psycopg2
from pathlib import Path
from configparser import Configparser
from ui.instr.storage import StorageDriver


class TestStorageDriver:
	def setup(self):
		self.cfg_path = Path("test_cfg.ini")
		self.cfg_data = {
		"database": "test_db_name",
		"user": "test_user_name",
		"password": "test_password_name",
		"host": "test_host_name"
		}

		cfg = Configparser()
		cfg["test_db_cfg"] = self.cfg_data
		with self.cfg_data.open("w") as cfg_file:
			cfg.write(cfg_file)
	def teardown(self):
		self.cfg_path.unlink()
