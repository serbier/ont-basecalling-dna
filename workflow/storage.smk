from utils.get_file import get_batch_ids_raw, get_signal_batch

def get_batches_indexing(wildcards):
    return expand("{data_raw}/{run_name}/reads/{batch}.fofn",
        data_raw = config["storage_data_raw"],
        run_name=wildcards.run_name,
        batch=get_batch_ids_raw(wildcards, config=config))


# extract read ID from individual fast5 files
rule storage_index_batch:
    input:
        batch = lambda wildcards : get_signal_batch(wildcards, config)
    output:
        temp("{data_raw}/{{run_name, [^.\/]*}}/reads/{{batch}}.fofn".format(data_raw = config["storage_data_raw"]))
    shadow: "shallow"
    threads: 1
    resources:
        threads = lambda wildcards, threads: threads,
        mem_mb = lambda wildcards, attempt: int((1.0 + (0.1 * (attempt - 1))) * 4000),
        time_min = 15
    shell:
        """
        python scripts/fast5index.py index {input.batch} --out_prefix reads --tmp_prefix $(pwd) > {output}
        """

# merge batch indices
rule storage_index_run:
    input:
        batches = get_batches_indexing
    output:
        fofn = "{data_raw}/{{run_name, [^.\/]*}}/reads.fofn".format(data_raw = config["storage_data_raw"])
    run:
        with open(output[0], 'w') as fp:
            for f in input.batches:
                print(open(f, 'r').read(), end='', file=fp)