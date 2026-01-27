version 1.0

workflow files_not_in_data_tables {
    input {
        Int cycle
        String center
    }

    call check_files_in_buckets {
        input:
            cycle = cycle,
            center = center
    }

    output {
        Array[File] file_list = check_files_in_buckets.file_list
    }
}


task check_files_in_buckets {
    input {
        Int cycle
        String center
    }

    command <<<
        wget https://raw.githubusercontent.com/UW-GAC/gregor-data-tracking/refs/heads/main/workspace_management/check_files_in_buckets.R
        Rscript check_files_in_buckets.R ~{cycle} ~{center}
    >>>

    output {
        Array[File] file_list = glob("*files_not_referenced_in_data_tables.tsv")
    }
}
