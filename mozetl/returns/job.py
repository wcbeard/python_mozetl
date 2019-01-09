import ast
import click
import bgbb
from bgbb import BGBB
from pyspark.sql import SparkSession

# Generate queries w/ date ranges
# These are at file:/dbfs/wbeard/bgbb_cli
from bgbb.sql.sql_utils import run_rec_freq_spk, to_samp_ids
from bgbb.sql.bgbb_udfs import mk_udfs

# samp_ids = to_samp_ids(range(10))


class PythonLiteralOption(click.Option):
    """
    allow passing click a list of floats
    https://stackoverflow.com/a/47730333/386279
    """

    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)


def extract(model_win=180, ho_start=None, sample_ids='0', spark=None):
    df, q = run_rec_freq_spk(
        HO_WIN=1, MODEL_WIN=model_win, ho_start=ho_start,
        sample_ids=sample_ids, spark=spark
    )
    return df


def transform(df, bgbb_params):
    # Instantiate model object w/ pre-selected parameters
    _pars = [0.825, 0.68, 0.0876, 1.385]
    bgbb = BGBB(params=_pars)

    # Create/Apply UDFs
    p_alive, n_returns = mk_udfs(bgbb, params=_pars, return_in_next_n_days=14, alive_n_days_later=0)

    df2 = (
        df
        .withColumn('P14', n_returns(dfs.Frequency, dfs.Recency, dfs['T']))
        .withColumn('P_alive', p_alive(dfs.Frequency, dfs.Recency, dfs['T']))
    )



@click.command('returns')
@click.option('--model-win', type=int, default=180)
@click.option('--sample-ids', type=str, default='0')
@click.option('--submission-date', type=str, required=True)
@click.option('--model-params', type=str, required=True)
def main(model_win, sample_ids, submission_date):
    spark = SparkSession.builder.getOrCreate()

    df = extract(
            model_win=model_win, samp_ids=samp_ids, ho_start=submission_date
    )
    print('bgbb!')

