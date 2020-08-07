import requests
import csv
import time
import json

base_url = 'https://app.dimensions.ai/api/'


def load_wun():
    wun = []
    with open('wun.csv', 'r') as unis:
        reader = csv.reader(unis, delimiter=',')
        for uni in reader:
            wun.append(uni[0])
    return wun


def load_wun_and_peers():
    wun_and_peers = []
    with open('wun_and_peers_grid_ids.csv', 'r') as institutions:
        reader = csv.reader(institutions, delimiter=',')
        line = 0
        for inst in reader:
            if line != 0:
                wun_and_peers.append(inst[0])
            line += 1
    return wun_and_peers


def load_grid_ids():
    with open('institutions.csv', 'r') as univ_grid:
        univ_reader = csv.reader(univ_grid, delimiter=',')
        univ_grid_ids = {}
        for univ in univ_reader:
            univ_grid_ids[univ[1]] = univ[0]
        return univ_grid_ids


def match_peers_grid_ids():
    with open('wun_and_peers.csv', 'r') as wun_and_peers, open('peers_grid_ids.csv', 'w', newline='') as peers_grid_ids:
        reader = csv.reader(wun_and_peers)
        writer = csv.writer(peers_grid_ids)
        grid_ids = load_grid_ids()
        line = 0
        for row in reader:
            if line != 0:
                try:
                    grid_id = grid_ids[row[0]]
                    writer.writerow(row + [grid_id])
                except KeyError:
                    print(f'couldn\'t find grid id for {row[0]}')
                    writer.writerow(row)
            else:
                writer.writerow(row + ['Grid ID'])
            line += 1


def get_related_institutions(grid_id):
    with open('relationships.csv', 'r') as rel,\
            open('institutions.csv', 'r') as inst:
        rel_reader = csv.reader(rel, delimiter=',')
        inst_reader = csv.reader(inst, delimiter=',')

        rel_inst_grid_ids = [row[2] for row in rel_reader if row[0] == grid_id]
        rel_inst_names_list = ["\"{}\"".format(row[1]) for row in inst_reader
                               for id in rel_inst_grid_ids if row[0] == id]
        return ', '.join(rel_inst_names_list)


def initialize_session():

    login = ''
    with open('config.json', 'r') as config:
        login = json.load(config)

    print('\nInitializing session...\n')

    resp = requests.post(base_url + 'auth.json', json=login)
    resp.raise_for_status()

    header = {
        'Authorization': 'JWT ' + resp.json()['token']
    }

    print('Session in progress...\n')

    return header


def query_institution_fors(header, years_range, institutions_list):
    # aggregate by geometric average of field citation ratio
    query = 'search publications where year in [{}] and research_orgs.name in [{}] '\
    'return FOR_first aggregate fcr_gavg limit 100'.format(years_range, institutions_list)

    response = requests.post(
        base_url + 'dsl.json',
        data=query.encode(),
        headers=header)
    time.sleep(1)

    return response.json()['FOR_first']


def scrape_institutions():
    # fors stands for Fields of Research
    with open('wun_and_peers_grid_ids.csv', 'r') as unis:
        reader = csv.reader(unis, delimiter=',')
        years_list = ["2013:201{}".format(year) for year in range(3, 9)]
        header = initialize_session()
        count = 0
        for uni in reader:
            if count != 0:
                uni_name = uni[0]
                grid_id = uni[1]
                rel_inst = get_related_institutions(grid_id)
                inst_list = ', '.join(["\"{}\"".format(uni_name), rel_inst]) if len(rel_inst) != 0 else "\"{}\"".format(uni_name)
                print(f'\nQuery nr {count}')
                print(f'List of institutions in query: {inst_list}')

                for years in years_list:
                    print(uni_name, years)
                    latest_year = years.split(':')[-1]
                    fors = query_institution_fors(header, years, inst_list)

                    with open('inst/{}/fors_{}_{}.csv'.format(latest_year, uni_name, latest_year), 'w', newline='') as fors_out:
                        writer = csv.writer(fors_out, delimiter=',')
                        writer.writerow(['FOR', 'FCR', 'Count'])
                        for field in fors:
                            row = [field['name'], field['fcr_gavg'], field['count']]
                            writer.writerow(row)

            count += 1
        print(f'successfully scraped data for {count-1} universities...')


def query_connection_fors(header, years_range, inst_list_1, inst_list_2):
    # aggregate by geometric average of field citation ratio
    query = 'search publications where year in [{}] and research_orgs.name in [{}] and research_orgs.name in '\
    '[{}] return FOR_first aggregate fcr_gavg limit 100'.format(years_range, inst_list_1, inst_list_2)

    response = requests.post(
        base_url + 'dsl.json',
        data=query.encode(),
        headers=header)
    time.sleep(1)

    response = response.json()
    if response['_stats']['total_count'] != 0:
        return response['FOR_first']
    else:
        return {}


def scrape_connections():
    header = initialize_session()
    # wun_and_peers = load_wun_and_peers()
    wun_and_peers = load_wun()
    grid_ids = load_grid_ids()
    years_list = ["2013:201{}".format(year) for year in range(3, 9)]
    count = 1
    # this checks all the possible undirected connections between any two institutions from the wun and peers list
    # the scraping has to end with a query on the last two elements of the list
    for i in range(len(wun_and_peers) - 1):
        uni_name_1 = wun_and_peers[i]
        rel_inst_1 = get_related_institutions(grid_ids[uni_name_1])
        inst_list_1 = ', '.join(["\"{}\"".format(uni_name_1), rel_inst_1]) if len(rel_inst_1) != 0 else "\"{}\"".format(uni_name_1)
        for j in range(1 + i, len(wun_and_peers)):
            uni_name_2 = wun_and_peers[j]
            rel_inst_2 = get_related_institutions(grid_ids[uni_name_2])
            inst_list_2 = ', '.join(["\"{}\"".format(uni_name_2), rel_inst_2]) if len(rel_inst_2) != 0 else "\"{}\"".format(uni_name_2)
            print('\nQuery nr {}: querying {} <-> {}...'.format(count, uni_name_1, uni_name_2))
            for years in years_list:
                print('\tduring {}'.format(years))
                latest_year = years.split(':')[-1]
                fors = query_connection_fors(header, years, inst_list_1, inst_list_2)

                with open('conns/{}/fors_{}_{}_{}.csv'.format(latest_year, uni_name_1, uni_name_2, latest_year), 'w', newline='') as fors_out:
                    writer = csv.writer(fors_out, delimiter=',')
                    writer.writerow(['FOR', 'FCR', 'Count'])
                    if len(fors) > 0:
                        for field in fors:
                            row = [field['name'], field['fcr_gavg'], field['count']]
                            writer.writerow(row)
                count += 1
    print(f'successfully scraped data for {count-1} connections...')


def main():
    # scrape_institutions()
    scrape_connections()


if __name__ == '__main__':
    main()
