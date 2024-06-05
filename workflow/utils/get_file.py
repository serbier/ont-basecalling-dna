import os
from snakemake.io import glob_wildcards



def get_batches_indexing(wildcards):
    return expand("{data_raw}/{run_name}/reads/{batch}.fofn",
        data_raw = config["storage_data_raw"],
        runname=wildcards.runname,
        batch=get_batch_ids_raw(wildcards, config=config))

def get_signal_batch(wildcards, config):
    raw_dir = config['storage_data_raw']
    
    batch_file = os.path.join(raw_dir, wildcards.run_name, 'reads', wildcards.batch)

    if os.path.isfile(batch_file + '.fast5'):
        return batch_file + '.fast5'
    else:
        return []


def get_batch_ids_raw(run_name, config):
	raw_dir = config['storage_data_raw']
	batches_fast5, = glob_wildcards("{datadir}/{run_name}/reads/{{id}}.fast5"
	.format(datadir=config["storage_data_raw"], run_name=run_name))

	return batches_fast5