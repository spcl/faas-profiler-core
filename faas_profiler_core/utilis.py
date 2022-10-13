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


def sec_to_ms(sec: float) -> float:
    """
    Converts seconds to milliseconds
    """
    return sec * 1000
