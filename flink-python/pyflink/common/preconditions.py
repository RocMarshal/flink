################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

__all__ = [
    'NoneTypeException',
    'IllegalArgumentException',
    'IllegalStateException',
    'IndexOutOfBoundsException',
    'Preconditions'
]


class NoneTypeException(Exception):
    def __init__(self, *args, **kwargs):  # real signature unknown
        pass


class IllegalArgumentException(Exception):
    def __init__(self, *args, **kwargs):  # real signature unknown
        pass


class IllegalStateException(Exception):
    def __init__(self, *args, **kwargs):  # real signature unknown
        pass


class IndexOutOfBoundsException(Exception):
    def __init__(self, *args, **kwargs):  # real signature unknown
        pass


class Preconditions(object):

    @staticmethod
    def check_not_null(reference, error_message=""):
        if reference is None:
            raise NoneTypeException(error_message)
        return reference

    @staticmethod
    def check_argument(condition, error_message=""):
        if not condition:
            raise IllegalArgumentException(error_message)

    @staticmethod
    def check_state(condition, error_message=""):
        if not condition:
            raise IllegalStateException(error_message)

    @staticmethod
    def check_state_return(condition, returned_value=None):
        if condition:
            return returned_value

    @staticmethod
    def check_element_index(index, size, error_message=""):
        Preconditions.check_state(size >= 0, "Size was negative.")
        if index < 0 or index >= size:
            raise IndexOutOfBoundsException(
                error_message + " Index: " + str(index) + ", Size: " + str(size))
