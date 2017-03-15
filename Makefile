PROJECT = mnesis
PROJECT_DESCRIPTION = Redis interface with mnesia
PROJECT_VERSION = 0.1.0

DEPS = ranch
LOCAL_DEPS = mnesia sasl observer runtime_tools wx

include erlang.mk
