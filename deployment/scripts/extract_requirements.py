import argparse
from configparser import ConfigParser


def cli_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extracts dependencies from setup.cfg into a requirements.txt file."
    )
    parser.add_argument(
        "config_file", action="store", nargs=1, type=str, help="Path to setup.cfg."
    )
    parser.add_argument(
        "--output-file",
        "-o",
        action="store",
        default="requirements.txt",
        type=str,
        help="Path to the requirements file to create.",
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    config_file = args.config_file
    output_file = args.output_file

    parser = ConfigParser()
    parser.read(config_file)

    requirements = parser["options"]["install_requires"]
    with open(output_file, "w") as f:
        f.write(requirements.strip())


if __name__ == "__main__":
    main(cli_parse())
