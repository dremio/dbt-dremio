import pytest
from dbt.tests.adapter.concurrency.test_concurrency import (
    BaseConcurrency,
)
from dbt.tests.util import (
    check_relations_equal,
    check_table_does_not_exist,
    rm_file,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data

models__invalid_sql = """
{{
  config(
    materialized = "table"
  )
}}

select a_field_that_does_not_exist from {{ ref(var('seed_name', 'seed')) }}

"""

models__table_a_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ ref(var('seed_name', 'seed')) }}

"""

models__table_b_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ ref(var('seed_name', 'seed')) }}

"""

models__view_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ ref(var('seed_name', 'seed')) }}

"""

models__dep_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ref('view_model')}}

"""

models__view_with_conflicting_cascade_sql = """
select * from {{ref('table_a')}}

union all

select * from {{ref('table_b')}}

"""

models__skip_sql = """
select * from {{ref('invalid')}}

"""

seeds__update_csv = """id,first_name,last_name,email,gender,ip_address
1,Michael,Perez,mperez0@chronoengine.com,Male,106.239.70.175
2,Shawn,Mccoy,smccoy1@reddit.com,Male,24.165.76.182
3,Kathleen,Payne,kpayne2@cargocollective.com,Female,113.207.168.106
4,Jimmy,Cooper,jcooper3@cargocollective.com,Male,198.24.63.114
5,Katherine,Rice,krice4@typepad.com,Female,36.97.186.238
6,Sarah,Ryan,sryan5@gnu.org,Female,119.117.152.40
7,Martin,Mcdonald,mmcdonald6@opera.com,Male,8.76.38.115
8,Frank,Robinson,frobinson7@wunderground.com,Male,186.14.64.194
9,Jennifer,Franklin,jfranklin8@mail.ru,Female,91.216.3.131
10,Henry,Welch,hwelch9@list-manage.com,Male,176.35.182.168
11,Fred,Snyder,fsnydera@reddit.com,Male,217.106.196.54
12,Amy,Dunn,adunnb@nba.com,Female,95.39.163.195
13,Kathleen,Meyer,kmeyerc@cdc.gov,Female,164.142.188.214
14,Steve,Ferguson,sfergusond@reverbnation.com,Male,138.22.204.251
15,Teresa,Hill,thille@dion.ne.jp,Female,82.84.228.235
16,Amanda,Harper,aharperf@mail.ru,Female,16.123.56.176
17,Kimberly,Ray,krayg@xing.com,Female,48.66.48.12
18,Johnny,Knight,jknighth@jalbum.net,Male,99.30.138.123
19,Virginia,Freeman,vfreemani@tiny.cc,Female,225.172.182.63
20,Anna,Austin,aaustinj@diigo.com,Female,62.111.227.148
21,Willie,Hill,whillk@mail.ru,Male,0.86.232.249
22,Sean,Harris,sharrisl@zdnet.com,Male,117.165.133.249
23,Mildred,Adams,madamsm@usatoday.com,Female,163.44.97.46
24,David,Graham,dgrahamn@zimbio.com,Male,78.13.246.202
25,Victor,Hunter,vhuntero@ehow.com,Male,64.156.179.139
26,Aaron,Ruiz,aruizp@weebly.com,Male,34.194.68.78
27,Benjamin,Brooks,bbrooksq@jalbum.net,Male,20.192.189.107
28,Lisa,Wilson,lwilsonr@japanpost.jp,Female,199.152.130.217
29,Benjamin,King,bkings@comsenz.com,Male,29.189.189.213
30,Christina,Williamson,cwilliamsont@boston.com,Female,194.101.52.60
31,Jane,Gonzalez,jgonzalezu@networksolutions.com,Female,109.119.12.87
32,Thomas,Owens,towensv@psu.edu,Male,84.168.213.153
33,Katherine,Moore,kmoorew@naver.com,Female,183.150.65.24
34,Jennifer,Stewart,jstewartx@yahoo.com,Female,38.41.244.58
35,Sara,Tucker,stuckery@topsy.com,Female,181.130.59.184
36,Harold,Ortiz,hortizz@vkontakte.ru,Male,198.231.63.137
37,Shirley,James,sjames10@yelp.com,Female,83.27.160.104
38,Dennis,Johnson,djohnson11@slate.com,Male,183.178.246.101
39,Louise,Weaver,lweaver12@china.com.cn,Female,1.14.110.18
40,Maria,Armstrong,marmstrong13@prweb.com,Female,181.142.1.249
41,Gloria,Cruz,gcruz14@odnoklassniki.ru,Female,178.232.140.243
42,Diana,Spencer,dspencer15@ifeng.com,Female,125.153.138.244
43,Kelly,Nguyen,knguyen16@altervista.org,Female,170.13.201.119
44,Jane,Rodriguez,jrodriguez17@biblegateway.com,Female,12.102.249.81
45,Scott,Brown,sbrown18@geocities.jp,Male,108.174.99.192
46,Norma,Cruz,ncruz19@si.edu,Female,201.112.156.197
47,Marie,Peters,mpeters1a@mlb.com,Female,231.121.197.144
48,Lillian,Carr,lcarr1b@typepad.com,Female,206.179.164.163
49,Judy,Nichols,jnichols1c@t-online.de,Female,158.190.209.194
50,Billy,Long,blong1d@yahoo.com,Male,175.20.23.160
51,Howard,Reid,hreid1e@exblog.jp,Male,118.99.196.20
52,Laura,Ferguson,lferguson1f@tuttocitta.it,Female,22.77.87.110
53,Anne,Bailey,abailey1g@geocities.com,Female,58.144.159.245
54,Rose,Morgan,rmorgan1h@ehow.com,Female,118.127.97.4
55,Nicholas,Reyes,nreyes1i@google.ru,Male,50.135.10.252
56,Joshua,Kennedy,jkennedy1j@house.gov,Male,154.6.163.209
57,Paul,Watkins,pwatkins1k@upenn.edu,Male,177.236.120.87
58,Kathryn,Kelly,kkelly1l@businessweek.com,Female,70.28.61.86
59,Adam,Armstrong,aarmstrong1m@techcrunch.com,Male,133.235.24.202
60,Norma,Wallace,nwallace1n@phoca.cz,Female,241.119.227.128
61,Timothy,Reyes,treyes1o@google.cn,Male,86.28.23.26
62,Elizabeth,Patterson,epatterson1p@sun.com,Female,139.97.159.149
63,Edward,Gomez,egomez1q@google.fr,Male,158.103.108.255
64,David,Cox,dcox1r@friendfeed.com,Male,206.80.80.58
65,Brenda,Wood,bwood1s@over-blog.com,Female,217.207.44.179
66,Adam,Walker,awalker1t@blogs.com,Male,253.211.54.93
67,Michael,Hart,mhart1u@wix.com,Male,230.206.200.22
68,Jesse,Ellis,jellis1v@google.co.uk,Male,213.254.162.52
69,Janet,Powell,jpowell1w@un.org,Female,27.192.194.86
70,Helen,Ford,hford1x@creativecommons.org,Female,52.160.102.168
71,Gerald,Carpenter,gcarpenter1y@about.me,Male,36.30.194.218
72,Kathryn,Oliver,koliver1z@army.mil,Female,202.63.103.69
73,Alan,Berry,aberry20@gov.uk,Male,246.157.112.211
74,Harry,Andrews,handrews21@ameblo.jp,Male,195.108.0.12
75,Andrea,Hall,ahall22@hp.com,Female,149.162.163.28
76,Barbara,Wells,bwells23@behance.net,Female,224.70.72.1
77,Anne,Wells,awells24@apache.org,Female,180.168.81.153
78,Harry,Harper,hharper25@rediff.com,Male,151.87.130.21
79,Jack,Ray,jray26@wufoo.com,Male,220.109.38.178
80,Phillip,Hamilton,phamilton27@joomla.org,Male,166.40.47.30
81,Shirley,Hunter,shunter28@newsvine.com,Female,97.209.140.194
82,Arthur,Daniels,adaniels29@reuters.com,Male,5.40.240.86
83,Virginia,Rodriguez,vrodriguez2a@walmart.com,Female,96.80.164.184
84,Christina,Ryan,cryan2b@hibu.com,Female,56.35.5.52
85,Theresa,Mendoza,tmendoza2c@vinaora.com,Female,243.42.0.210
86,Jason,Cole,jcole2d@ycombinator.com,Male,198.248.39.129
87,Phillip,Bryant,pbryant2e@rediff.com,Male,140.39.116.251
88,Adam,Torres,atorres2f@sun.com,Male,101.75.187.135
89,Margaret,Johnston,mjohnston2g@ucsd.edu,Female,159.30.69.149
90,Paul,Payne,ppayne2h@hhs.gov,Male,199.234.140.220
91,Todd,Willis,twillis2i@businessweek.com,Male,191.59.136.214
92,Willie,Oliver,woliver2j@noaa.gov,Male,44.212.35.197
93,Frances,Robertson,frobertson2k@go.com,Female,31.117.65.136
94,Gregory,Hawkins,ghawkins2l@joomla.org,Male,91.3.22.49
95,Lisa,Perkins,lperkins2m@si.edu,Female,145.95.31.186
96,Jacqueline,Anderson,janderson2n@cargocollective.com,Female,14.176.0.187
97,Shirley,Diaz,sdiaz2o@ucla.edu,Female,207.12.95.46
98,Nicole,Meyer,nmeyer2p@flickr.com,Female,231.79.115.13
99,Mary,Gray,mgray2q@constantcontact.com,Female,210.116.64.253
100,Jean,Mcdonald,jmcdonald2r@baidu.com,Female,122.239.235.117
"""


class TestConcurrency(BaseConcurrency):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid.sql": models__invalid_sql,
            "table_a.sql": models__table_a_sql,
            "table_b.sql": models__table_b_sql,
            "view_model.sql": models__view_model_sql,
            "dep.sql": models__dep_sql,
            "view_with_conflicting_cascade.sql": models__view_with_conflicting_cascade_sql,
            "skip.sql": models__skip_sql,
        }

    def test_concurrency(self, project):
        run_dbt(["seed", "--select", "seed"])
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        rm_file(project.project_root, "seeds", "seed.csv")
        write_file(seeds__update_csv, project.project_root, "seeds", "seed.csv")

        results, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output
