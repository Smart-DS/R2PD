import click
import h5py
import pandas as pds


def extract_h5(h5_path):
    with h5py.File(h5_path, 'r') as f:
        vars = list(f)
        data_ds = [ds for ds in vars if ds != 'loc_data'][0]

        data = pds.DataFrame(f[data_ds][...])

    cols = list(data.columns)
    if 'Timestamp' in cols:
        index_col = 'Timestamp'
    elif 'time' in cols:
        index_col = 'time'
    else:
        raise RuntimeError('Cannot determine time-index column')

    time_index = data[index_col].str.decode('utf-8')
    data[index_col] = pds.to_datetime(time_index)
    data = data.set_index(index_col)


@click.group()
def cli():
    pass


@cli.command()
@click.argument('h5_path', type=click.Path(exists=True))
def extract_hdf5(h5_path):
    extract_h5(h5_path)
    out_file = h5_path.replace('.hdf5', '.csv')
    click.echo('{} converted to {}'.format(h5_path, out_file))


if __name__ == '__main__':
    cli()
