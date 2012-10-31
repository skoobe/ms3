#!/bin/bash

virtualenv .
. bin/activate && pip install -r requirements.txt
. bin/activate && pip install -r requirements.dev.txt

echo "To switch to the development environment type:"
echo "$ . bin/activate"
echo "In order to exit, type:"
echo "$ deactivate"
