import sys
import os.path
import unittest2

p = os.path
sys.path.insert(0, p.normpath(p.join(p.dirname(p.abspath(__file__)), '..')))


def run():
    unittest2.main()
