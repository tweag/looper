from typing import Optional

import pydantic
import pydantic_argparse


## pydantic model for `looper run` command
class RunParser(pydantic.BaseModel):
    # arguments
    looper_config: str = pydantic.Field(description="Looper project configuration file")


## pydantic model for base command
class TopLevelParser(pydantic.BaseModel):
    # Commands
    run: Optional[RunParser] = pydantic.Field(description="Run a looper project")


def main() -> None:
    parser = pydantic_argparse.ArgumentParser(
        model=TopLevelParser,
        prog="looper",
        description="pydantic-argparse demo",
        add_help=True
    )
    args = parser.parse_typed_args()
    print(args)


if __name__ == "__main__":
    main()
