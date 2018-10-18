import click
import h5py
import pandas as pd


def extract_h5(h5_path):
    with h5py.File(h5_path, 'r') as f:
        vars = list(f)
        data_ds = [ds for ds in vars if ds != 'loc_data'][0]

        data = pd.DataFrame(f[data_ds][...])

    data['time'] = pd.to_datetime(data['time'].values.astype(str))
    data = data.set_index('time')

    out_path = h5_path.replace('.hdf5', '.csv')
    data.to_csv(out_path)


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
