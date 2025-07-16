#!/usr/bin/env python3
"""
Main entry point - redirects to server.py
This file exists as a fallback in case Railway looks for main.py
"""

if __name__ == '__main__':
    import server
    server.start_server()
