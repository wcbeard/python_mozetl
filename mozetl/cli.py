import click

from mozetl.returns import job as returns_job


@click.group()
def entry_point():
    pass

entry_point.add_command(returns_job.main, "returns")

# # @click.group()
# @click.command()
# def main():
#     print('hello world')


if __name__ == '__main__':
    # main(auto_envvar_prefix='MOZETL')
    entry_point()
