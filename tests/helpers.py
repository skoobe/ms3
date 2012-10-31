import sys
import os.path

p = os.path
sys.path.insert(0, p.normpath(p.join(p.dirname(p.abspath(__file__)), '..')))
