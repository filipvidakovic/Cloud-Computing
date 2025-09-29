from aws_cdk import Duration, aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX

class MusicLambdas(Construct):
    def __init__(self, scope: Construct, id: str, music_table, song_table, artist_info_table, s3_bucket,rates_table):
        super().__init__(scope, id)

        # music_table = MUSIC_BY_GENRE_TABLE (genre index)
        # song_table  = SONG_TABLE (canonical)
        env_vars_common = {
            "MUSIC_BY_GENRE_TABLE": music_table.table_name,
            "SONG_TABLE": song_table.table_name,
            "ARTIST_INFO_TABLE": artist_info_table.table_name,
            "S3_BUCKET": s3_bucket.bucket_name,
            "RATES_TABLE": rates_table.table_name
        }

        # ---------- Upload ----------
        self.upload_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}UploadMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="upload_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        song_table.grant_write_data(self.upload_music_lambda)
        music_table.grant_write_data(self.upload_music_lambda)
        artist_info_table.grant_write_data(self.upload_music_lambda)
        s3_bucket.grant_put(self.upload_music_lambda)

        # ---------- Get albums by genre (reads index + songs) ----------
        self.get_albums_by_genre_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetAlbumsByGenreLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_albums_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        music_table.grant_read_data(self.get_albums_by_genre_lambda)
        song_table.grant_read_data(self.get_albums_by_genre_lambda)
        s3_bucket.grant_read(self.get_albums_by_genre_lambda)

        # ---------- Get music details (by genre + musicId; reads both tables) ----------
        self.get_music_details_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_music_details.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(10),
        )
        music_table.grant_read_data(self.get_music_details_lambda)
        song_table.grant_read_data(self.get_music_details_lambda)

        # ---------- Delete a single song (deletes song + all index rows + S3) ----------
        self.delete_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(10),
        )
        song_table.grant_read_write_data(self.delete_music_lambda)
        music_table.grant_read_write_data(self.delete_music_lambda)
        s3_bucket.grant_delete(self.delete_music_lambda)

        # ---------- Delete all songs by artist (scans SONG_TABLE; deletes index rows + S3) ----------
        self.delete_artist_songs_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteArtistSongsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_artist_songs.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        song_table.grant_read_write_data(self.delete_artist_songs_lambda)
        music_table.grant_read_write_data(self.delete_artist_songs_lambda)
        s3_bucket.grant_delete(self.delete_artist_songs_lambda)

        # ---------- Update song (updates SONG_TABLE; syncs index; S3 put/delete) ----------
        self.update_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}UpdateMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="update_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        song_table.grant_read_write_data(self.update_music_lambda)
        music_table.grant_read_write_data(self.update_music_lambda)
        s3_bucket.grant_put(self.update_music_lambda)
        s3_bucket.grant_delete(self.update_music_lambda)

        # ---------- Batch get by musicIds (reads SONG_TABLE only; presigns S3) ----------
        self.batch_get_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}BatchGetMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_music_batch_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )

        # Grant read access to DynamoDB
        rates_table.grant_read_data(self.batch_get_music_lambda)
        music_table.grant_read_data(self.batch_get_music_lambda)
        song_table.grant_read_data(self.batch_get_music_lambda)
        s3_bucket.grant_read(self.batch_get_music_lambda)


        self.download_song_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DownloadSongLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="download_song.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment={
                **env_vars_common
            },
            timeout=Duration.seconds(10),
        )
        # Needs to read song metadata + presign S3
        song_table.grant_read_data(self.download_song_lambda)
        s3_bucket.grant_read(self.download_song_lambda)

