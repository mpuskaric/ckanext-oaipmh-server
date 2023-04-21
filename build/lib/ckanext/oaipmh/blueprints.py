import flask
from flask import Blueprint
from flask import make_response
from ckan.plugins import toolkit
import logging

import oaipmh.metadata as oaimd
import oaipmh.server as oaisrv
#from pylons import request, response

#from ckan.lib.base import BaseController, render
from ckanext.oaipmh.oaipmh_server import CKANServer
#from rdftools import rdf_reader, dcat2rdf_writer
from ckanext.oaipmh.datacite_writer import datacite_writer
#from ckanext.oaipmh.eudatcore_writer import eudatcore_writer

log = logging.getLogger(__name__)

oai = Blueprint('oai', __name__)

@oai.route('/oai', endpoint='b2find_oai', methods=['POST', 'GET'])

def b2find_oai():
    log.info(flask.request.args)    # GET [ flask.request.args == toolkit.request.params ] 
    log.info(flask.request.form)    # POST
    if 'verb' in flask.request.args:
        verb = flask.request.args['verb'] if flask.request.args['verb'] else None
        if verb:
            client = CKANServer()
            metadata_registry = oaimd.MetadataRegistry()
            metadata_registry.registerReader('oai_dc', oaimd.oai_dc_reader)
            metadata_registry.registerWriter('oai_dc', oaisrv.oai_dc_writer)
            ## metadata_registry.registerReader('rdf', rdf_reader)
            ## metadata_registry.registerWriter('rdf', dcat2rdf_writer)
            metadata_registry.registerWriter('oai_datacite', datacite_writer)
            ##metadata_registry.registerWriter('oai_eudatcore', eudatcore_writer)
            serv = oaisrv.BatchingServer(client,
                                         metadata_registry=metadata_registry,
                                         resumption_batch_size=500)
            ##parms = toolkit.request.params.mixed()
            parms = flask.request.args
            res = serv.handleRequest(parms)
            ##toolkit.response.headers['content-type'] = 'text/xml; charset=utf-8'
            response = make_response(res)
            ##response.headers['content-type'] = 'text/xml; charset=utf-8'
            response.headers["content-type"] = "text/xml; charset=UTF-8"
            return response
    elif 'verb' in flask.request.form:
        verb = flask.request.form['verb'] if flask.request.form['verb'] else None
        if verb:
            client = CKANServer()
            metadata_registry = oaimd.MetadataRegistry()
            metadata_registry.registerReader('oai_dc', oaimd.oai_dc_reader)
            metadata_registry.registerWriter('oai_dc', oaisrv.oai_dc_writer)
            metadata_registry.registerWriter('oai_datacite', datacite_writer)
            serv = oaisrv.BatchingServer(client,
                                         metadata_registry=metadata_registry,
                                         resumption_batch_size=500)
            parms=flask.request.form
            res = serv.handleRequest(parms)
            response = make_response(res)
            response.headers["content-type"] = "text/xml; charset=UTF-8"
            return response
    else:
        client = CKANServer()
        metadata_registry = oaimd.MetadataRegistry()
        metadata_registry.registerReader('oai_dc', oaimd.oai_dc_reader)
        metadata_registry.registerWriter('oai_dc', oaisrv.oai_dc_writer)
        metadata_registry.registerWriter('oai_datacite', datacite_writer)
        serv = oaisrv.BatchingServer(client,
                                         metadata_registry=metadata_registry,
                                         resumption_batch_size=500)
        parms=flask.request.args
        res = serv.handleRequest(parms)
        response = make_response(res)
        response.headers["content-type"] = "text/xml; charset=UTF-8"
        return response

    #return {} # TODO: if not verb 
