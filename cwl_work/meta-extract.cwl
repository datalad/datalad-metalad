cwlVersion: v1.0
class: CommandLineTool
baseCommand: ["datalad", "meta-extract"]
stdout: cwl.output.json
inputs:
  tr_record:
    - type: record
      name: traversal_record
      fields:
        - name: status
          type: string
        - name: type
          type: string
        - name: top_level_path
          type: string
        - name: path
          type: string
        - name: dataset_path
          type: string
        - name: dataset_id
          type: string
        - name: dataset_version
          type: string
        - name: root_dataset_id
          type: string?
        - name: root_dataset_version
          type: string?

  extractor:
    type: string
    inputBinding:
      position: 1
  file_path:
    type: string?
    inputBinding:
      position: 2

outputs:
  type: Any
