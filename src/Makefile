PYTHON?=python3

exe:
	$(PYTHON) -m pip install -r requirements.txt --user
	$(PYTHON) -OO -m PyInstaller -F -n "research_bundle" ./example_main.py --add-data defaults:defaults
