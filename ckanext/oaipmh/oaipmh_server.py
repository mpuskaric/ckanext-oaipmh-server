'''OAI-PMH implementation for CKAN datasets and groups.
'''
# pylint: disable=E1101,E1103
from datetime import datetime,timedelta

from ckan.model import Package, Session, Group
from ckan.lib.helpers import url_for
from ckan.logic import get_action

#from pylons import config
from ckan.plugins import toolkit

from sqlalchemy import between

#pyoai
from oaipmh.common import ResumptionOAIPMH
from oaipmh import common
from oaipmh.error import IdDoesNotExistError
import ckanext.oaipmh.utils as utils

import logging

log = logging.getLogger(__name__)


class CKANServer(ResumptionOAIPMH):
    '''A OAI-PMH implementation class for CKAN.
    '''
    def identify(self):
        '''Return identification information for this server.
        '''
        return common.Identify(
            repositoryName=toolkit.config.get('ckan.site_title', 'repository'),
            #baseURL=url_for(controller='ckanext.oaipmh.controller:OAIPMHController',action='index'),
	    baseURL=toolkit.config.get('ckan.site_url', None) + '/oai',
            protocolVersion="2.0",
            adminEmails=['puskaric@hlrs.de'],
            earliestDatestamp=utils.get_earliest_datestamp(),
            deletedRecord='no',
            granularity='YYYY-MM-DDThh:mm:ssZ',
            #granularity='YYYY-MM-DD',
            compression=['identity'])

    def _get_json_content(self, js):
        '''
        Gets all items from JSON
        :param js: json string
        :return: list of items
        '''
        try:
            json_data = json.loads(js)
            json_titles = list()
            for key, value in json_data.items():
                if value:
                    json_titles.append(value)
            return json_titles
        except:
            return [js]
			
    def _record_for_dataset_dcat(self, dataset, set_spec):
        '''Show a tuple of a header and metadata for this dataset.
        Note that dataset_xml (metadata) returned is just a string containing
        ready rdf xml. This is contrary to the common practice of pyoia's
        getRecord method.
        '''
        package = get_action('package_show')({}, {'id': dataset.id})
        dataset_xml = rdfserializer.serialize_dataset(package, _format='xml')
        return (common.Header('', dataset.name, dataset.metadata_modified, set_spec, False),
                dataset_xml, None)

    def _record_for_dataset_datacite(self, dataset, set_spec):
        '''Show a tuple of a header and metadata for this dataset.
        '''
        package = get_action('package_show')({}, {'id': dataset.id})
        # Loops through extras -table:
        extras = {}
        for item in package['extras']:
            for key, value in item.items():
                key = item['key']   # extras table is constructed as key: language, value: English
                value = item['value']  # instead of language : English, that is why it is looped here
                if key in ['spatial']:
                    extras.update({key: value})
                else:
                    values = value.split(";")
                    extras.update({key: values})

        temporal_begin = extras.get('TemporalCoverage:BeginDate')
        temporal_end = extras.get('TemporalCoverage:EndDate')
        dates = []
        if temporal_begin or temporal_end:
            begin = temporal_begin[0] if temporal_begin else ''
            end = temporal_end[0] if temporal_end else ''
            dates.append("%s/%s" % (begin, end))

        # identifiers = self._set_id(package, extras)
        subj = [tag.get('display_name') for tag in package['tags']] if package.get('tags', None) else None
        if subj is not None and 'Discipline' in extras:
            subj.extend(extras['Discipline'])

        author = package.get('author')
        if author:
            authors = [a for a in author.split(";")]
        else:
            authors = None

        meta = {
            'DOI': extras['DOI'] if 'DOI' in extras else None,
            'PID': extras['PID'] if 'PID' in extras else None,
            'version': extras['Version'] if 'Version' in extras else None,
            'source': package.get('url', None),
            'relatedIdentifier': extras['RelatedIdentifier'] if 'RelatedIdentifier' in extras else None,
            'creator': authors if authors else None,
            'publisher': extras['Publisher'] if 'Publisher' in extras else None,
            'publicationYear': extras['PublicationYear'] if 'PublicationYear' in extras else None,
            'publicationTimestamp': extras['PublicationTimestamp'] if 'PublicationTimestamp' in extras else None,
            'resourceType': extras['ResourceType'] if 'ResourceType' in extras else None,
            'language': extras['Language'] if 'Language' in extras else None,
            'titles': package.get('title', None) or package.get('name'),
            'contributor': extras['Contributor'] if 'Contributor' in extras else None,
            'descriptions': self._get_json_content(package.get('notes')) if package.get('notes', None) else None,
            'subjects': subj,
            'rights': extras['Rights'] if 'Rights' in extras else None,
            'openAccess': extras['OpenAccess'] if 'OpenAccess' in extras else None,
            'size': extras['Size'] if 'Size' in extras else None,
            'format': extras['Format'] if 'Format' in extras else None,
            'dates': dates if dates else None,
        }

        metadata = {}
        # Fixes the bug on having a large dataset being scrambled to individual
        # letters
        for key, value in meta.items():
            if value and not isinstance(value, list):
                metadata[str(key)] = [value]
            else:
                metadata[str(key)] = value
        #base_url, identifier = self._provinfo(extras['MetaDataAccess'][0])
        return (common.Header('', dataset.name, dataset.metadata_modified, set_spec, False),
                common.Metadata('', metadata),
                #common.About('', '', '',dataset.metadata_modified, ','.join(extras.get('repositoryID', [])), ','.join(extras.get('repositoryName', [])))
                common.About('', '', '', '', ','.join(extras.get('repositoryID', [])), ','.join(extras.get('repositoryName', [])))
                )

    def _record_for_dataset_dc(self, dataset, set_spec):
        '''Show a tuple of a header and metadata for this dataset.
        '''
        package = get_action('package_show')({}, {'id': dataset.id}) #name of the dataset
        # Loops through extras -table:
        extras = {}
        for item in package['extras']:
            for key, value in item.items():
                key = item['key']   # extras table is constructed as key: language, value: English
                value = item['value']  # instead of language : English, that is why it is looped here
                values = value.split(";")
                extras.update({key: values})
        '''
        coverage = []
        temporal_begin = package.get('temporal_coverage_begin', '')
        temporal_end = package.get('temporal_coverage_end', '')

        geographic = package.get('geographic_coverage', '')
        if geographic:
            coverage.extend(geographic.split(','))
        if temporal_begin or temporal_end:
            coverage.append("%s/%s" % (temporal_begin, temporal_end))
        '''
        pids = []
        if 'DOI' in extras:
            pids.append(extras['DOI'][0])
        if 'PID' in extras:
            pids.append(extras['PID'][0])
        url = package.get('url', None)
        if url:
            pids.append(url)

        subj = [tag.get('display_name') for tag in package['tags']] if package.get('tags', None) else None
        if subj is not None and 'Discipline' in extras:
            subj.extend(extras['Discipline'])

        author = package.get('author')
        if author:
            authors = [a for a in author.split(";")]
        else:
            authors = None
        log.info("_record_for_dataset_dc")
        meta = {#'title': self._get_json_content(package.get('title', None) or package.get('name')),
                #'identifier': package.get('id'),
                'identifier': extras['URL'] if 'URL' in extras else None,
                'url': extras['URL'] if 'URL' in extras else None,
                'type': ['dataset'],
                'language': [l.strip() for l in package.get('language').split(",")] if package.get('language', None) else None,
                'description': self._get_json_content(package.get('notes')) if package.get('notes', None) else None,
                'subject': [tag.get('display_name') for tag in package['tags']] if package.get('tags', None) else None,
                'creator': [tag.get('display_name') for tag in package['tags']] if package.get('tags', None) else None,
                'date': [dataset.metadata_modified.strftime('%Y-%m-%d')] if dataset.metadata_modified else None,
                #'rights': [package['license_title']] if package.get('license_title', None) else None,
                'publisher': extras['Publisher'] if 'Publisher' in extras else None,
                'creator': authors if authors else None,
                'contributor': extras['Contributor'] if 'Contributor' in extras else None,
                'rights': extras['Rights'] if 'Rights' in extras else None,
                'size': extras['Size'] if 'Size' in extras else None,
                #'format': extras['Format'] if 'Format' in extras else None,
                'format': [resources.get('format') for resources in package['resources']] if package.get('resources', None) else None,
                'title': package.get('title', None) or package.get('name'), }
        log.info(meta)
        iters = dict(dataset.extras.items())
        meta.update(iters)
        metadata = {}
        # Fixes the bug on having a large dataset being scrambled to individual
        # letters
        for key, value in meta.items():
            if not isinstance(value, list):
                metadata[str(key)] = [value]
            else:
                metadata[str(key)] = value
        #base_url, identifier = self._provinfo(extras['MetaDataAccess'][0])
        return (common.Header('', package.get('id'), dataset.metadata_modified, set_spec, False),
                common.Metadata('', metadata),
                #common.About('', '', '',dataset.metadata_modified, ','.join(extras.get('repositoryID', [])), ','.join(extras.get('repositoryName', [])))
                #common.About('', '', '', '', ','.join(extras.get('repositoryID', [])), ','.join(extras.get('repositoryName', [])))
                common.About('', '', '', '', '', '')
                )
				
    @staticmethod
    def _filter_packages(set, cursor, from_, until, batch_size):
        '''Get a part of datasets for "listNN" verbs.
        '''
        packages = []
        if not set:
            packages = Session.query(Package).filter(Package.type=='dataset'). \
                filter(Package.state == 'active').filter(Package.private!=True)
            if from_ and not until:
                packages = packages.filter(Package.metadata_modified > from_)
            if until and not from_:
                #log.info(type(until))
                until = until.strftime("%Y-%m-%dT%H:%M:%SZ")
                parsed_until=datetime.strptime(until, "%Y-%m-%dT%H:%M:%SZ")
                updated_until=parsed_until + timedelta(seconds=1)
                #log.info(type(parsed_date))
                packages = packages.filter(Package.metadata_modified < updated_until)
            if from_ and until:
                #packages = packages.filter(between(Package.metadata_modified, from_, until))
                until = until.strftime("%Y-%m-%dT%H:%M:%SZ")
                parsed_until=datetime.strptime(until, "%Y-%m-%dT%H:%M:%SZ")
                updated_until=parsed_until + timedelta(seconds=1)
                packages = packages.filter(between(Package.metadata_modified, from_, updated_until))
            if batch_size:
                packages = packages.limit(batch_size)
            if cursor:
                packages = packages.offset(cursor)
            packages = packages.all()
        else:
            group = Group.get(set)
            if group:
                # Note that group.packages never returns private datasets regardless of 'with_private' parameter.
                packages = group.packages(return_query=True, with_private=False).filter(Package.type=='dataset'). \
                    filter(Package.state == 'active')
                if from_ and not until:
                    packages = packages.filter(Package.metadata_modified > from_)
                if until and not from_:
                    log.info(until)
                    packages = packages.filter(Package.metadata_modified <= until)
                if from_ and until:
                    packages = packages.filter(between(Package.metadata_modified, from_, until))
                if batch_size:
                    packages = packages.limit(batch_size)
                if cursor:
                    packages = packages.offset(cursor)
                packages = packages.all()
        # if cursor is not None:
        #     cursor_end = cursor + batch_size if cursor + batch_size < len(packages) else len(packages)
        #     packages = packages[cursor:cursor_end]
        return packages
		
    @staticmethod
    def _set_spec(package):
        set_spec = []
        if package.owner_org:
            group = Group.get(package.owner_org)
            if group and group.name:
                if not group.name == "eudat-b2find":
                    set_spec.append(group.name)
        # if not set_spec:
        #    set_spec = [package.name]
        return set_spec
		
    def getRecord(self, metadataPrefix, identifier):
        '''Simple getRecord for a dataset.
        '''
        package = Package.get(identifier)
        if not package:
            raise IdDoesNotExistError("No dataset with id %s" % identifier)

        set_spec = self._set_spec(package)
        if metadataPrefix == 'rdf':
            return self._record_for_dataset_dcat(package, set_spec)
        if metadataPrefix == 'oai_datacite':
            return self._record_for_dataset_datacite(package, set_spec)
        return self._record_for_dataset_dc(package, set_spec)

    def listIdentifiers(self, metadataPrefix=None, set=None, cursor=None,
                        from_=None, until=None, batch_size=None):
        '''List all identifiers for this repository.
        '''
        data = []
        packages = self._filter_packages(set, cursor, from_, until, batch_size)

        for package in packages:
            set_spec = self._set_spec(package)
            data.append(common.Header('', package.id, package.metadata_modified, set_spec, False))
        return data
		
    def listMetadataFormats(self, identifier=None):
        '''List available metadata formats.
        '''
        return [('oai_dc',
                 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                 'http://www.openarchives.org/OAI/2.0/oai_dc/'),
                ('oai_datacite',
                 'http://schema.datacite.org/meta/kernel-4.3/metadata.xsd',
                 'http://datacite.org/schema/kernel-4'),
                # ('rdf',
                # 'http://www.openarchives.org/OAI/2.0/rdf.xsd',
                # 'http://www.openarchives.org/OAI/2.0/rdf/')
                ]
				
    def listRecords(self, metadataPrefix=None, set=None, cursor=None, from_=None,
                    until=None, batch_size=None):
        '''Show a selection of records, basically lists all datasets.
        '''
        data = []
        packages = self._filter_packages(set, cursor, from_, until, batch_size)

        for package in packages:
            set_spec = self._set_spec(package)
            if metadataPrefix == 'rdf':
                data.append(self._record_for_dataset_dcat(package, set_spec))
            elif metadataPrefix == 'oai_datacite':
                data.append(self._record_for_dataset_datacite(package, set_spec))
            else:
                data.append(self._record_for_dataset_dc(package, set_spec))
        return data

    def listSets(self, cursor=None, batch_size=None):
        '''List all sets in this repository, where sets are groups.
        '''
        data = []
        groups = Session.query(Group).filter(Group.state == 'active')
        if cursor is not None:
            cursor_end = cursor+batch_size if cursor+batch_size < groups.count() else groups.count()
            groups = groups[cursor:cursor_end]
        for group in groups:
            data.append((group.name, group.title, group.description))
        return data
