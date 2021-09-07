cwlVersion: v1.0
class: Workflow
inputs:
  dataset: string
  extractor: string

outputs:
  metadata:
    type: string
    outputSource: extract/metadata_record

steps:
  traverse:
    run: traverse.cwl
    in:
      dataset: dataset
    out: [traverse_record]

  extract:
    run: meta-extract.cwl
    in:
      dataset: dataset
      extractor: extractor
      file_path: traverse/traverse_record
    out: [metadata_record]
