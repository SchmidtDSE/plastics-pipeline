.mode csv
.import {{target_dir}}01productionofresinnofiber.csv file_01productionofresinnofiber
.import {{target_dir}}02productionfiber.csv file_02productionfiber
.import {{target_dir}}03productionadditives.csv file_03productionadditives
.import {{target_dir}}04netimportresinnofibercopy.csv file_04netimportresinnofibercopy
.import {{target_dir}}05netimportfibercopy.csv file_05netimportfibercopy
.import {{target_dir}}06netimportplasticarticlescopy.csv file_06netimportplasticarticlescopy
.import {{target_dir}}07netimportplasticinfinishedgoodsnofibercopy.csv file_07netimportplasticinfinishedgoodsnofibercopy
{% for region in regions %}
.import {{target_dir}}{{ region["files"]["endUseAndType"] }}.csv file_{{ region["files"]["endUseAndType"] }}
{% endfor %}
.import {{target_dir}}12lifetimecopy.csv file_12lifetimecopy
.import {{target_dir}}1319502004copy.csv file_1319502004copy
{% for region in regions %}
.import {{target_dir}}{{ region["files"]["eol"] }}.csv file_{{ region["files"]["eol"] }}
{% endfor %}
{% for region in regions %}
.import {{target_dir}}{{ region["files"]["trade"] }}.csv file_{{ region["files"]["trade"] }}
{% endfor %}
.import {{target_dir}}22wastetrade.csv file_22wastetrade
.import {{target_dir}}23historic.csv file_23historic
