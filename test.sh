#!/bin/bash

for f in *.csv; do
  python myscript.py "$f" "${f%.csv}"
done