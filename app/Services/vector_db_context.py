from typing import Optional

import numpy
from loguru import logger
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models
from qdrant_client.models import RecommendStrategy

from app.Models.api_model import SearchModelEnum, SearchBasisEnum
from app.Models.img_data import ImageData
from app.Models.query_params import FilterParams
from app.Models.search_result import SearchResult
from app.config import config


class PointNotFoundError(ValueError):
    def __init__(self, point_id: str):
        self.point_id = point_id
        super().__init__(f"Point {point_id} not found.")


class VectorDbContext:
    IMG_VECTOR = "image_vector"
    TEXT_VECTOR = "text_contain_vector"
    AVAILABLE_POINT_TYPES = models.Record | models.ScoredPoint | models.PointStruct

    def __init__(self):
        self._client = AsyncQdrantClient(host=config.qdrant.host, port=config.qdrant.port,
                                         grpc_port=config.qdrant.grpc_port, api_key=config.qdrant.api_key,
                                         prefer_grpc=config.qdrant.prefer_grpc)
        self.collection_name = config.qdrant.coll

    async def retrieve_by_id(self, image_id: str, with_vectors=False) -> ImageData:
        """
        Retrieve an item from database by id. Will raise PointNotFoundError if the given ID doesn't exist.
        :param image_id: The ID to retrieve.
        :param with_vectors: Whether to retrieve vectors.
        :return: The retrieved item.
        """
        logger.info("Retrieving item {} from database...", image_id)
        result = await self._client.retrieve(collection_name=self.collection_name,
                                             ids=[image_id],
                                             with_payload=True,
                                             with_vectors=with_vectors)
        if len(result) != 1:
            logger.error("Point not exist.")
            raise PointNotFoundError(image_id)
        return self._get_img_data_from_point(result[0])

    async def retrieve_by_ids(self, image_id: list[str], with_vectors=False) -> list[ImageData]:
        """
        Retrieve items from the database by IDs.
        An exception is thrown if there are items in the IDs that do not exist in the database.
        :param image_id: The list of IDs to retrieve.
        :param with_vectors: Whether to retrieve vectors.
        :return: The list of retrieved items.
        """
        logger.info("Retrieving {} items from database...", len(image_id))
        result = await self._client.retrieve(collection_name=self.collection_name,
                                             ids=image_id,
                                             with_payload=True,
                                             with_vectors=with_vectors)
        result_point_ids = {t.id for t in result}
        missing_point_ids = set(image_id) - result_point_ids
        if len(missing_point_ids) > 0:
            logger.error("{} points not exist.", len(missing_point_ids))
            raise PointNotFoundError(str(missing_point_ids))
        return self._get_img_data_from_points(result)

    async def validate_ids(self, image_id: list[str]) -> list[str]:
        """
        Validate a list of IDs. Will return a list of valid IDs.
        :param image_id: The list of IDs to validate.
        :return: The list of valid IDs.
        """
        logger.info("Validating {} items from database...", len(image_id))
        result = await self._client.retrieve(collection_name=self.collection_name,
                                             ids=image_id,
                                             with_payload=False,
                                             with_vectors=False)
        return [t.id for t in result]

    async def querySearch(self, query_vector, query_vector_name: str = IMG_VECTOR,
                          top_k=10, skip=0, filter_param: FilterParams | None = None) -> list[SearchResult]:
        logger.info("Querying Qdrant... top_k = {}", top_k)
        result = await self._client.search(collection_name=self.collection_name,
                                           query_vector=(query_vector_name, query_vector),
                                           query_filter=self._get_filters_by_filter_param(filter_param),
                                           limit=top_k,
                                           offset=skip,
                                           with_payload=True)
        logger.success("Query completed!")
        return [self._get_search_result_from_scored_point(t) for t in result]

    async def querySimilar(self,
                           query_vector_name: str = IMG_VECTOR,
                           search_id: Optional[str] = None,
                           positive_vectors: Optional[list[numpy.ndarray]] = None,
                           negative_vectors: Optional[list[numpy.ndarray]] = None,
                           mode: Optional[SearchModelEnum] = None,
                           with_vectors: bool = False,
                           filter_param: FilterParams | None = None,
                           top_k: int = 10,
                           skip: int = 0) -> list[SearchResult]:
        _positive_vectors = [t.tolist() for t in positive_vectors] if positive_vectors is not None else [search_id]
        _negative_vectors = [t.tolist() for t in negative_vectors] if negative_vectors is not None else None
        _strategy = None if mode is None else (RecommendStrategy.AVERAGE_VECTOR if
                                               mode == SearchModelEnum.average else RecommendStrategy.BEST_SCORE)
        # since only combined_search need return vectors, We can define _combined_search_need_vectors like below
        _combined_search_need_vectors = [
            self.IMG_VECTOR if query_vector_name == self.TEXT_VECTOR else self.IMG_VECTOR] if with_vectors else None
        logger.info("Querying Qdrant... top_k = {}", top_k)
        result = await self._client.recommend(collection_name=self.collection_name,
                                              using=query_vector_name,
                                              positive=_positive_vectors,
                                              negative=_negative_vectors,
                                              strategy=_strategy,
                                              with_vectors=_combined_search_need_vectors,
                                              query_filter=self._get_filters_by_filter_param(filter_param),
                                              limit=top_k,
                                              offset=skip,
                                              with_payload=True)
        logger.success("Query completed!")

        return [self._get_search_result_from_scored_point(t) for t in result]

    async def insertItems(self, items: list[ImageData]):
        logger.info("Inserting {} items into Qdrant...", len(items))

        points = [self._get_point_from_img_data(t) for t in items]

        response = await self._client.upsert(collection_name=self.collection_name,
                                             wait=True,
                                             points=points)
        logger.success("Insert completed! Status: {}", response.status)

    async def deleteItems(self, ids: list[str]):
        logger.info("Deleting {} items from Qdrant...", len(ids))
        response = await self._client.delete(collection_name=self.collection_name,
                                             points_selector=models.PointIdsList(
                                                 points=ids
                                             ),
                                             )
        logger.success("Delete completed! Status: {}", response.status)

    async def updatePayload(self, new_data: ImageData):
        """
        Update the payload of an existing item in the database.
        Warning: This method will not update the vector of the item.
        :param new_data: The new data to update.
        """
        response = await self._client.set_payload(collection_name=self.collection_name,
                                                  payload=new_data.payload,
                                                  points=[str(new_data.id)],
                                                  wait=True)
        logger.success("Update completed! Status: {}", response.status)

    async def updateVectors(self, new_points: list[ImageData]):
        resp = await self._client.update_vectors(collection_name=self.collection_name,
                                                 points=[self._get_vector_from_img_data(t) for t in new_points],
                                                 )
        logger.success("Update vectors completed! Status: {}", resp.status)

    async def scroll_points(self,
                            from_id: str | None = None,
                            count=50,
                            with_vectors=False) -> tuple[list[ImageData], str]:
        resp, next_id = await self._client.scroll(collection_name=self.collection_name,
                                                  limit=count,
                                                  offset=from_id,
                                                  with_vectors=with_vectors
                                                  )

        return [self._get_img_data_from_point(t) for t in resp], next_id

    async def get_counts(self, exact: bool) -> int:
        resp = await self._client.count(collection_name=self.collection_name, exact=exact)
        return resp.count

    @classmethod
    def _get_vector_from_img_data(cls, img_data: ImageData) -> models.PointVectors:
        vector = {}
        if img_data.image_vector is not None:
            vector[cls.IMG_VECTOR] = img_data.image_vector.tolist()
        if img_data.text_contain_vector is not None:
            vector[cls.TEXT_VECTOR] = img_data.text_contain_vector.tolist()
        return models.PointVectors(
            id=str(img_data.id),
            vector=vector
        )

    @classmethod
    def _get_point_from_img_data(cls, img_data: ImageData) -> models.PointStruct:
        return models.PointStruct(
            id=str(img_data.id),
            payload=img_data.payload,
            vector=cls._get_vector_from_img_data(img_data).vector
        )

    @classmethod
    def _get_img_data_from_point(cls, point: AVAILABLE_POINT_TYPES) -> ImageData:
        return (ImageData
                .from_payload(point.id,
                              point.payload,
                              image_vector=numpy.array(point.vector[cls.IMG_VECTOR], dtype=numpy.float32)
                              if point.vector and cls.IMG_VECTOR in point.vector else None,
                              text_contain_vector=numpy.array(point.vector[cls.TEXT_VECTOR], dtype=numpy.float32)
                              if point.vector and cls.TEXT_VECTOR in point.vector else None
                              ))

    @classmethod
    def _get_img_data_from_points(cls, points: list[AVAILABLE_POINT_TYPES]) -> list[ImageData]:
        return [cls._get_img_data_from_point(t) for t in points]

    @classmethod
    def _get_search_result_from_scored_point(cls, point: models.ScoredPoint) -> SearchResult:
        return SearchResult(img=cls._get_img_data_from_point(point), score=point.score)

    @classmethod
    def getVectorByBasis(cls, basis: SearchBasisEnum) -> str:
        match basis:
            case SearchBasisEnum.vision:
                return cls.IMG_VECTOR
            case SearchBasisEnum.ocr:
                return cls.TEXT_VECTOR
            case _:
                raise ValueError("Invalid basis")

    @staticmethod
    def _get_filters_by_filter_param(filter_param: FilterParams | None) -> models.Filter | None:
        if filter_param is None:
            return None

        filters = []
        neg_filter = []
        if filter_param.min_width is not None and filter_param.min_width > 0:
            filters.append(models.FieldCondition(
                key="width",
                range=models.Range(
                    gte=filter_param.min_width
                )
            ))

        if filter_param.min_height is not None and filter_param.min_height > 0:
            filters.append(models.FieldCondition(
                key="height",
                range=models.Range(
                    gte=filter_param.min_height
                )
            ))

        if filter_param.min_ratio is not None:
            filters.append(models.FieldCondition(
                key="aspect_ratio",
                range=models.Range(
                    gte=filter_param.min_ratio,
                    lte=filter_param.max_ratio
                )
            ))

        if filter_param.starred is not None:
            filters.append(models.FieldCondition(
                key="starred",
                match=models.MatchValue(
                    value=filter_param.starred
                )
            ))

        if filter_param.ocr_text is not None:
            filters.append(models.FieldCondition(
                key="ocr_text_lower",
                match=models.MatchText(
                    text=filter_param.ocr_text.lower()
                )
            ))

        if filter_param.categories is not None:
            filters.append(models.FieldCondition(
                key="categories",
                match=models.MatchAny(
                    any=filter_param.categories
                )
            ))

        if filter_param.categories_negative is not None:
            neg_filter.append(models.FieldCondition(
                key="categories",
                match=models.MatchAny(any=filter_param.categories_negative),
            ))

        if not filters and not neg_filter:
            return None
        return models.Filter(
            must=filters,
            must_not=neg_filter
        )
