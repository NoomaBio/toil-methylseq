import math
from enum import IntEnum
from dataclasses import dataclass
from typing import List

from toil.fileStores import FileID


@dataclass
class ResourceRequirement:
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

    def __init__(self, amount: int, unit: str):
        self.amount = amount
        self.unit = unit

    def to_string(self):
        return f"{self.amount}{self.unit}"

    def __add__(self, other):
        assert self.unit == other.unit, "can't add different units.."
        amount = self.amount + other.amount
        return ResourceRequirement(amount=amount, unit=self.unit)

    @classmethod
    def convert_size(cls, size_bytes: int):
        if size_bytes == 0:
            return ResourceRequirement(amount=0, unit="B")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = int(round(size_bytes / p, 2))
        return ResourceRequirement(amount=s, unit=cls.size_name[i])

    def convert_to(self, other_unit: str):
        assert other_unit in self.size_name, f"don't understand {other_unit} unit"
        x = self.size_name.index(self.unit)
        y = self.size_name.index(other_unit)
        diff = x - y
        mul = 1000 ** diff
        new_amount = self.amount * mul
        return ResourceRequirement(amount=new_amount, unit=other_unit)


@dataclass
class ArtifactResourceRequirements:
    memory: ResourceRequirement
    disc: ResourceRequirement

    def __str__(self):
        return f"memory: {self.memory.to_string()}, disc: {self.disc.to_string()}"

    def __repr__(self):
        return str(self)

    def __add__(self, other):
        if isinstance(other, int):
            assert other == 0
            mem_default = ResourceRequirement(amount=0, unit=self.memory.unit)
            disc_default = ResourceRequirement(amount=0, unit=self.disc.unit)
            return ArtifactResourceRequirements(memory=mem_default, disc=disc_default)

        memory = self.memory + other.memory
        disc = self.disc + other.disc
        return ArtifactResourceRequirements(memory=memory, disc=disc)

    def __radd__(self, other):
        return self + other

    def __mul__(self, other):
        assert isinstance(other, int)
        memory = self.memory.amount * other
        disc = self.disc.amount * other
        return ArtifactResourceRequirements(
            memory=ResourceRequirement(amount=memory, unit=self.memory.unit),
            disc=ResourceRequirement(amount=disc, unit=self.disc.unit),
        )


class Storage(IntEnum):
    S3 = 1
    Local = 2

    @classmethod
    def parse(cls, raw: str):
        if raw.lower() == "s3":
            return Storage.S3
        if raw.lower() == "file":
            return Storage.Local
        raise ValueError(f"unrecognized storage {raw}")


@dataclass
class S3OutputLocation:
    bucket: str
    key: str

    @classmethod
    def parse(cls, raw: str):
        try:
            _, rest = raw.split("s3://")
            bucket, *rest = rest.split("/")
            return S3OutputLocation(bucket=bucket, key="/".join(rest))
        except ValueError as e:
            raise ValueError(f"failed to parse s3 location {raw}") from e

    def to_url(self, filename: str) -> str:
        key = self.key if self.key.startswith("/") else f"/{self.key}"
        filename = filename.replace("/", "")
        return f"s3://{self.bucket}{key}/{filename}"


@dataclass
class PairedEndReads:
    name: str
    uri_1: str
    uri_2: str
    storage: Storage

    @classmethod
    def parse(cls, raw: dict):
        if len(raw.keys()) > 1:
            raise ValueError(
                "illegal input, should be 1 key, the identifier of the reads"
            )
        for name, reads in raw.items():
            try:
                uri_1 = reads["1"]
                uri_2 = reads["2"]
                storage_raw = uri_1.split(":")[0]
                if storage_raw != uri_2.split(":")[0]:
                    raise ValueError(f"file schemes for {name} don't match")
                storage = Storage.parse(storage_raw)

                return PairedEndReads(
                    name=name, uri_1=uri_1, uri_2=uri_2, storage=storage
                )
            except KeyError as e:
                raise ValueError(
                    "illegal input, paired reads should have '1' and '2' keys with"
                    "uris as values"
                ) from e


@dataclass
class PairedEndReadShard:
    mate1_fid: FileID
    mate2_fid: FileID
    name: str


@dataclass
class ToilMethylseqConfig:
    paired_reads: List[PairedEndReads]
    s3_output: S3OutputLocation
    apps_image: str
    utils_image: str
    bismark_index_url: str
    bismark_genome_uri: str
    bins: int
