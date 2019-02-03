<?php

namespace SMW\SQLStore\EntityStore;

use SMW\SQLStore\SQLStore;
use SMW\SQLStore\PropertyTableDefinition as TableDefinition;
use SMWDataItem as DataItem;
use SMW\DIWikiPage;
use SMW\DIProperty;
use SMW\RequestOptions;
use SMW\DataTypeRegistry;
use RuntimeException;
use SMW\MediaWiki\LinkBatch;

/**
 * @license GNU GPL v2
 * @since 3.1
 *
 * @author mwjames
 */
class BulkLoader {

	/**
	 * @var SQLStore
	 */
	private $store;

	/**
	 * @var SemanticDataLookup
	 */
	private $semanticDataLookup;

	/**
	 * @var PropertySubjectsLookup
	 */
	private $propertySubjectsLookup;

	/**
	 * @var LinkBatch
	 */
	private $linkBatch;

	/**
	 * @var []
	 */
	private $cache = [];

	/**
	 * @var []
	 */
	private $lookupLog = [];

	/**
	 * @since 3.1
	 *
	 * @param SQLStore $store
	 */
	public function __construct( SQLStore $store, CachingSemanticDataLookup $semanticDataLookup, PropertySubjectsLookup $propertySubjectsLookup, LinkBatch $linkBatch = null ) {
		$this->store = $store;
		$this->semanticDataLookup = $semanticDataLookup;
		$this->propertySubjectsLookup = $propertySubjectsLookup;
		$this->linkBatch = $linkBatch;

		// Help reduce the amount of queries by allowing the prefetch those
		// links we know will be used for the display
		if ( $this->linkBatch === null ) {
			$this->linkBatch = new LinkBatch();
		}
	}

	/**
	 * @since 3.1
	 *
	 * @param DIProperty $property
	 *
	 * @return boolean
	 */
	public function isCached( DIProperty $property ) {
		return isset( $this->cache[$property->getKey()] );
	}

/*


{{#ask: [[PropChain::+]]
 |?PropChain
 |?PropChain.PropChain=PropChain|+order=desc
 |?PropChain.PropChain.PropChain=PropChain
 |?PropChain.-PropChain=-PropChain
 |?PropChain.-PropChain.Has number=Has number
 |?PropChain.-PropChain.Has subobject.Has number=Has number
 |format=broadtable
 |limit=50
 |offset=0
 |link=all
 |sort=
 |order=asc
 |headers=show
 |searchlabel=... further results
 |class=sortable wikitable smwtable
}}
 */

	/**
	 * @since 3.1
	 *
	 * {@inheritDoc}
	 */
	public function load( array $subjects, DIProperty $property, RequestOptions $requestOptions ) {

		$fingerprint = '';

		foreach ( $subjects as $subject ) {
			$fingerprint .= $subject->getHash();
			$this->linkBatch->add( $subject );
		}

		$hash = md5(
			$fingerprint .
			$property->getKey() .
			$property->isInverse() .
			$requestOptions->isChain
		);

		if ( isset( $this->lookupLog[$hash] ) ) {
			return;
		}

		$result = [];
		$prop = $property->getKey();
		$idTable = $this->store->getObjectIds();

		// Use the .dot notation to distingish it from other prrintouts that
		// use the same property
		if ( isset( $requestOptions->isChain ) && $requestOptions->isChain ) {
			$prop = $requestOptions->isChain;
		}

		if ( $property->isInverse() ) {
			$noninverse = new DIProperty(
				$property->getKey(),
				false
			);

			$type = DataTypeRegistry::getInstance()->getDataItemByType(
				$noninverse->findPropertyTypeID()
			);

			$tableid = $this->store->findPropertyTableID( $noninverse );

			if ( $tableid === '' ) {
				return [];
			}

			$proptables = $this->store->getPropertyTables();

			$ids = [];

			foreach ( $subjects as $s ) {
				$sid = $idTable->getSMWPageID(
					$s->getDBkey(),
					$s->getNamespace(),
					$s->getInterwiki(),
					$s->getSubobjectName(),
					true
				);

				if ( $type !== $s->getDIType() || $sid == 0 ) {
					continue;
				}

				$s->setId( $sid );
				$ids[] = $sid;

				// $result[$sid] = $this->store->getPropertySubjects( $noninverse, $s, $requestOptions );
			}

			$result = $this->propertySubjectsLookup->prefetchFromTable(
				$ids,
				$property,
				$proptables[$tableid],
				$requestOptions
			);

		} else {
			$tableid = $this->store->findPropertyTableID( $property );

			if ( $tableid === '' ) {
				return [];
			}

			$proptables = $this->store->getPropertyTables();

			// Doing a bulk request to eliminate DB requests to match the
			// values of a single subject by relying on a `... WHERE IN ...`
			$data = $this->semanticDataLookup->prefetchSemanticData(
				$subjects,
				$property,
				$proptables[$tableid],
				$requestOptions
			);

			$result = isset( $this->cache[$prop] ) ? $this->cache[$prop] : [];
			//$result = [];
			$list = [];

			$propertyTypeId = $property->findPropertyTypeID();
			$propertyDiId = DataTypeRegistry::getInstance()->getDataItemId( $propertyTypeId );

			foreach ( $data as $sid => $dbkeys ) {

				// Store by related SID, the caller is responsible to reassign the
				// results to a corresponding output
				if ( !isset( $result[$sid] ) ) {
					$result[$sid] = [];
				}

				foreach ( $dbkeys as $k => $v ) {
					try {
						$diHandler = $this->store->getDataItemHandlerForDIType( $propertyDiId );
						$dataItem = $diHandler->dataItemFromDBKeys( $v );
						$list[] = $dataItem;
						// Apply uniqueness
						$result[$sid][$dataItem->getHash()] = $dataItem;
						$this->linkBatch->add( $dataItem );
					} catch ( SMWDataItemException $e ) {
						// maybe type assignment changed since data was stored;
						// don't worry, but we can only drop the data here
					}
				}
			}

			// Give the collective list of subjects a chance to warm up the cache and eliminate
			// DB requests to find a matching ID for each individual entity item
			if ( $propertyDiId === \SMWDataItem::TYPE_WIKIPAGE ) {
				$this->store->getObjectIds()->warmUpCache( $list );
			}
		}

		$this->linkBatch->execute();

		$this->cache[$prop] = $result;
		$this->lookupLog[$hash] = true;
	}

	public function get( DIWikiPage $subject, DIProperty $property ) {

		$prop = $property->getKey();

		$sid = $this->store->getObjectIds()->getSMWPageID(
			$subject->getDBkey(),
			$subject->getNamespace(),
			$subject->getInterwiki(),
			$subject->getSubobjectName(),
			true
		);

		if ( !isset( $this->cache[$prop][$sid] ) ) {
			return [];
		}

		return array_values( $this->cache[$prop][$sid] );
	}

}
