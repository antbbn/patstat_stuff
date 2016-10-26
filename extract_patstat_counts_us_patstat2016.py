#!/usr/bin/python

"""
.. module:: find_patstat_applications_mysql
.. moduleauthor:: T'Mir D. Julius <tjulius@swin.edu.au>

:Copyright: T'Mir D. Julius for The Centre for Transformative Innovation
Swinburne University of Technology, 2015
:Contact: T'Mir D. Julius <tdj@tdjulius.com>
:Updated: 23/02/2015
"""


def main():

    db = mt.get_db1()
    cur = db.cursor()

    # patstat_appln_id -> appln_id
    # year(appln_filing_date) -> appln_filing_year
    #print cur.execute("""SELECT patstat_appln_id, year(appln_filing_date), year(publn_first_grant_date) FROM  """
    #                 """mondelez_2014_patstat_appln WHERE appln_auth = 'US' """)
    print cur.execute("""SELECT appln_id, appln_filing_year, granted FROM  """
                      """tls201_appln WHERE appln_auth = 'US' """)
    results = cur.fetchall()

    year_dict_reg = dict()
    year_dict_filed = dict()

    outfile_filed = open("data/us_filed.txt", 'w')
    outfile_reg = open("data/us_reg.txt", 'w')

    outfile_reg.write('|'.join(['reg_year', 'ipc', 'applt_ctry', 'count', 'ipc_weighted_count', 'applt_weighted_count',
                                'ipc_applt_weighted_count']) + '\r\n')

    outfile_filed.write('|'.join(['filing_year', 'ipc', 'applt_ctry', 'count', 'ipc_weighted_count', 'applt_weighted_count',
                                'ipc_applt_weighted_count']) + '\r\n')

    i = 0

    for result in results:
        #nr, filing, reg = result
        nr, filing, granted = result
        i += 1
        if i % 100 == 0:
            print i

        if granted:
        	cur.execute("""SELECT year(publn_date) FROM tls221_pat_publn WHERE appln_id=%s AND publn_first_grant=1 """, [nr])
        	reg = cur.fetch()
        else:
        	reg = None


        #cur.execute("""SELECT applt_ctry_code FROM mondelez_2014_patstat_applt where patstat_appln_id = %s """, [nr])
        cur.execute("""SELECT p.person_ctry_code FROM tls2016_person as p, tls207_pers_appln as pa WHERE p.person_id = pa.person_id AND pa.appln_id = %s """, [nr])

        applt_ctrys = [x[0] for x in cur.fetchall()]

        #cur.execute("""SELECT left(ipc_class_symbol, 4) FROM mondelez_2014_patstat_ipc where patstat_appln_id = %s """, [nr])
        cur.execute(""""SELECT left(ipc_class_symbol, 4) FROM tls209_appln_ipc where appln_id = %s """, [nr])

        ipcs = [x[0] for x in cur.fetchall()]

        n_ctrys = len(applt_ctrys)
        n_ipcs = len(ipcs)

        for country in applt_ctrys:
            for ipc in ipcs:
                new_dict = year_dict_filed.get(filing, dict())
                ipc_dict = new_dict.get(ipc, dict())
                ipc_dict[country] = tuple([sum(x) for x in
                                           zip(*[ipc_dict.get(country, (0, 0, 0 ,0)),
                                                 [1, 1.0/n_ctrys, 1.0/n_ipcs, 1.0/n_ctrys/n_ipcs]])])

                new_dict[ipc] = ipc_dict
                year_dict_filed[filing] = new_dict

                if reg is not None:
                    new_dict = year_dict_reg.get(reg, dict())
                    ipc_dict = new_dict.get(ipc, dict())
                    ipc_dict[country] = tuple([sum(x) for x in
                                               zip(*[ipc_dict.get(country, (0, 0, 0 ,0)),
                                                     [1, 1.0/n_ctrys, 1.0/n_ipcs, 1.0/n_ctrys/n_ipcs]])])

                    new_dict[ipc] = ipc_dict
                    year_dict_reg[reg] = new_dict

    for year, vals0 in year_dict_filed.items():
        for ipc, vals1 in vals0.items():
            for ctry, vals2 in vals1.items():
                outfile_filed.write('|'.join([str(x) for x in [year, ipc, ctry] + list(vals2)]) + '\r\n')

    for year, vals0 in year_dict_reg.items():
        for ipc, vals1 in vals0.items():
            for ctry, vals2 in vals1.items():
                outfile_reg.write('|'.join([str(x) for x in [year, ipc, ctry] + list(vals2)]) + '\r\n')

    outfile_reg.close()
    outfile_filed.close()


if __name__ == '__main__':
    import sys
    import mysql_common.mysql_tools as mt

    reload(sys)
    sys.setdefaultencoding("utf-8")
    sys.path.append("/home/tjulius/pythonstuff/modules")
    main()

