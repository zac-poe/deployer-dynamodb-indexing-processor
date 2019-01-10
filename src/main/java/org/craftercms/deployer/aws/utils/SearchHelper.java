/*
 * Copyright (C) 2007-2019 Crafter Software Corporation. All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.craftercms.deployer.aws.utils;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.craftercms.search.service.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

/**
 * Performs general operations related to search.
 *
 * @author joseross
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SearchHelper {
	private static final String ID_FIELD = "id";

    private static final Logger logger = LoggerFactory.getLogger(SearchHelper.class);

    /**
     * Mapper used to generate documents as XML.
     */
    protected XmlMapper xmlMapper;

    public SearchHelper() {
        xmlMapper = new XmlMapper();
    }


    public void delete(SearchService searchService, String site,
                       com.amazonaws.services.dynamodbv2.model.Record record) {
    	Map oldImage = record.getDynamodb().getOldImage();
		if(oldImage == null) {
        	logger.error("Unable to delete doc without old image from site '{}'!", site);
        	return;
		}
		String id = ItemUtils.toItem(oldImage).getString("id");
		if(StringUtils.isEmpty(id)) {
        	logger.error("Unable to delete doc from site '{}' with no field '{}' defined!", site, ID_FIELD);
        	return;
		}
        searchService.delete(site, site, id);
    }

    /**
     * Updates the search index for a given document.
     * @param searchService search service instance
     * @param siteName the site name
     * @param map document fields as a map
     * @throws Exception if the update fails
     */
    public void update(SearchService searchService, String siteName, Map map) throws Exception {
        // Id need to be removed because searchService will generate it.
        String id = (String) map.remove(ID_FIELD);
        if(StringUtils.isEmpty(id)){
        	logger.error("Unable to index doc for site '{}' with no field '{}' defined!", siteName, ID_FIELD);
        	return;
        }
        logger.debug("Indexing doc with id '{}'", id);
        String xml = xmlMapper.writeValueAsString(map);
        searchService.update(siteName, siteName, id, xml, true);
    }

    /**
     * Transforms a Kinesis Data Stream record to a map.
     * @param record record to transform
     * @return values as a map
     */
    public Map getDocFromKinesis(Record record) {
        //TODO: Get values from raw data, not needed for now...
        throw new UnsupportedOperationException();
    }

    /**
     * Transforms a DynamoDB Record to a map.
     * @param record record to transform
     * @return values as a map
     */
    public Map getDocFromDynamo(com.amazonaws.services.dynamodbv2.model.Record record) {
        return ItemUtils.toItem(record.getDynamodb().getNewImage()).asMap();
    }

}
