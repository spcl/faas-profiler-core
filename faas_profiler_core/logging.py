#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logging
"""

import logging


class Loggable:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
