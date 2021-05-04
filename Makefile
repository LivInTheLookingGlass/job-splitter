PYTHON?=python3
ENTRY?=./example_main.py

exe:
	$(PYTHON) -m pip install -r requirements.txt --user
	$(PYTHON) -OO -m PyInstaller -F -n "research_bundle" $(ENTRY) --add-data src/defaults:src/defaults

html:
	cd docs && $(MAKE) html
