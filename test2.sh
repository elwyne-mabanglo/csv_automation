#!/bin/bash

for f in *.csv; do
  python C:/Users/Elwyne/Documents/python_project/CSV_Automation/CSV_Automation/domain_analysis.py "$f" "${f%.csv}"
done