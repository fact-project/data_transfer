import click
import pandas as pd
import os
from fact.path import template_to_path


@click.command()
@click.argument('in_path', type=click.Path(exists=True, dir_okay=False))
@click.argument('out_path', type=click.Path(exists=False, dir_okay=True))
@click.option('--key', help='Key of the data base in the hdf file', default="data")
def main(in_path, out_path, key):

    # outpath = '/net/big-tank/POOL/projects/fact/data/raw/'
    df = pd.read_csv(in_path)

    df["path"] = df.apply(
        lambda row: template_to_path(
            row['night'],
            row['run_id'],
            "/fact/raw/{Y}/{M}/{D}/{N}_{R}.*"
            ),
        axis=1
        )

    # drs_files = pd.DataFrame(df.night.unique(), columns=["night"])
    # drs_files["path"] = drs_files.apply(
    #     lambda row: template_to_path(
    #         row['night'],
    #         1,
    #         "/fact/raw/{Y}/{M}/{D}/{N}_*.drs.fits.gz"
    #         ),
    #     axis =1
    #     )

    # patterns = pd.concat([df[["path", "night"]], drs_files[["path", "night"]]])
    patterns = df
    patterns.sort_values(["night"], inplace=True)
    for night, grp in patterns.groupby("night"):
        # print("staging", pattern)
        stage_command = 'ssh isdcnx1 head -c 1 {}'.format(' '.join(list(grp['path'].values)))
        print(stage_command)
        os.system(stage_command)
        night_str = str(night)
        outputpath = os.path.join(out_path, night_str[:4], night_str[4:6], night_str[6:8])
        make_command = 'mkdir -p {}'.format(outputpath)
        print(make_command)
        os.system(make_command)
        rsync_drs_command = 'rsync -av --stats --progress isdcnx1:{} {}'.format(
            os.path.join(os.path.dirname(grp['path'].iloc[0]),"*.drs.fits.gz"),
            outputpath
            )
        print(rsync_drs_command)
        os.system(rsync_drs_command)

        for pattern in grp['path']:
            rsync_command = 'rsync -av --stats --progress isdcnx1:{} {}'.format(pattern, outputpath)
            print(rsync_command)
            os.system(rsync_command)

# rsync -av --stats --progress
# /fhgfs/groups/app/fact/raw/
# -m --include '*/'
# --include='20160101_185.fits.fz'
# --exclude='*' /net/big-tank/POOL/projects/fact/data/raw/


if __name__ == '__main__':
    main()
