require 'open-uri'
require 'uri'
require 'curation_concerns'
require 'tufts/vocab/tufts_terms'
require 'rdf/fcrepo3'

module FedoraMigrate
  class TuftsTargetConstructor
    attr_accessor :source, :candidates, :target
    attr_accessor :payload_primary, :payload_secondary, :depositor_utln

    def initialize(source, payload_stream, secondary_payload_stream, depositor)
      @source = source
      @payload_primary = payload_stream
      @payload_secondary = secondary_payload_stream
      @depositor_utln = depositor
    end

    # Override .build to use Fedora's default id minter
    # https://github.com/projecthydra-labs/fedora-migrate/issues/49
    #obj.id FedoraMigrate::Mover.id_component(source)
    def build
      raise FedoraMigrate::Errors::MigrationError, "No qualified targets found in #{source.pid}" if target.nil?

      # create target, and apply depositor metadata
      obj = target.new

      obj.apply_depositor_metadata @depositor_utln
      obj.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC

      user = User.find_by_user_key(@depositor_utln)
      CurationConcerns::Workflow::ActivateObject.call(target: obj, comment: 'activate object', user: user)

      create_and_add_payload(obj, @payload_primary, @depositor_utln)

      #deal with 2 primary datastream objects, storing second object in a new file set
      create_and_add_payload(obj, @payload_secondary, @depositor_utln) unless @payload_secondary.nil?

      #handle a case of bad hand created data on old records
      create_and_add_payload(obj, "ARCHIVAL_SOUND", @depositor_utln) if @payload_primary == "ARCHIVAL_WAV"

      # back up old data
      #create_and_add_fcrepo3_set obj

      process_desc_metadata obj
      process_admin_metadata obj
      process_collection_metadata obj
      process_relsext_metadata obj

      obj.save

      active_workflow = Sipity::Workflow.find(2)
      Sipity::Entity.create!(proxy_for_global_id: obj.to_global_id.to_s,
                             workflow: active_workflow,
                             workflow_state: nil)

      obj
    end

    def create_and_add_payload(obj, payload_stream, depositor_utln)
      # create fileset and apply depository metadata

      return if source.datastreams[payload_stream].content.nil?

      file_set = FileSet.new
      file_set.apply_depositor_metadata depositor_utln

      if payload_stream == "GENERIC-CONTENT"
        @doc = Nokogiri::XML(source.datastreams[payload_stream].content).remove_namespaces!
        location = @doc.xpath('//link').text
        uri = URI.parse(location)
        target_file = File.basename(uri.path)
        file_set.title = [target_file]
      else
        file_set.title = [source.datastreams[payload_stream].label]
      end

      file_set.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
      #set_voting_record_type file_set
      if payload_stream == "GENERIC-CONTENT"
        datastream_content = get_generic_file_from_source(payload_stream)
      else
        datastream_content = get_file_from_source(payload_stream)
      end


      user = User.find_by_user_key(file_set.depositor)
      actor = CurationConcerns::Actors::FileSetActor.new(file_set, user)
      actor.create_metadata(obj)
      actor.create_content(datastream_content)
      file_set.save

      #clean up temp file
      File.delete(datastream_content) if File.exist?(datastream_content)

    end

    def process_admin_metadata obj

    end

    def process_collection_metadata obj

    end

    def process_relsext_metadata obj

      xml = Nokogiri::XML(source.datastreams["RELS-EXT"].content).remove_namespaces!

      RDF::FCREPO3::RELSEXT.properties.each do |property|

        val = property.value
        short_code = val[val.index('#')+1..val.length]
        field_values = xml.xpath("//#{short_code}")
        target_values = Array.new

        field_values.each do |field|
          (target_values << field.values.first) unless field.nil?
        end

        obj.send("relsext_#{short_code}=", target_values) unless target_values.length == 0

      end
    end

    def process_desc_metadata obj
      obj.title = process_metadata_field 'dc:title'
      obj.creator = process_metadata_field 'dc:creator'
      obj.contributor = process_metadata_field 'dc:contributor'
      obj.description = process_metadata_field 'dc:description'
      obj.publisher = process_metadata_field 'dc:publisher'
      obj.source = process_metadata_field 'dc:source'
      obj.language = process_metadata_field 'dc:language'
      obj.legacy_pid = source.pid


    end

    def process_metadata_field field_name
      xml = Nokogiri::XML(source.datastreams["DCA-META"].content)
      field_values = xml.xpath("//#{field_name}")
      target_values = Array.new
      field_values.each do |field|
        (target_values << field.text) unless field.nil?
      end

      target_values
    end

    def target
      @target ||= determine_target
    end

    private

    def get_file_from_source(datastream)
      uri = URI.parse(source.datastreams[datastream].location)
      target_file = File.basename(uri.path)

      voting_record = File.new target_file, 'wb'
      voting_record.write source.datastreams[datastream].content.body
      #byebug
      File.new target_file

    end

    def get_generic_file_from_source(datastream)
      @doc = Nokogiri::XML(source.datastreams[datastream].content).remove_namespaces!
      location = @doc.xpath('//link').text

      uri = URI.parse(location)
      target_file = File.basename(uri.path)

      voting_record = File.new target_file, 'wb'
      voting_record.write Net::HTTP.get(uri)

      File.new target_file
    end

    def create_and_add_fcrepo3_set obj
      %w{DC RELS-EXT DCA-ADMIN DCA-META DC-DETAIL-META}.each_with_index do |ds, i|
        next if source.datastreams[ds].content.nil?
        fcrepo3_set = FileSet.new
        fcrepo3_set.apply_depositor_metadata @depositor_utln
        fcrepo3_set.title = ['Fedora 3 Datastreams']
        fcrepo3_set.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE

        # Have DC go first so there's always an 'original' ds

        user = User.find_by_user_key(fcrepo3_set.depositor)
        ds_actor = CurationConcerns::Actors::FileSetActor.new(fcrepo3_set, user)
        File.open(ds+'.xml', 'w') { |f| f.write(source.datastreams[ds].content) }
        ds_file = File.open(ds+'.xml', 'r:UTF-8')
        ds_actor.create_metadata(obj, {"visibility" => Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE})
        ds_actor.create_content(File.open(ds_file))
        fcrepo3_set.save
      end


    end

    def determine_target

      Array(candidates).map { |model| vet(model) }.compact.first
    end

    def vet(model)
      FedoraMigrate::Mover.id_component(model).constantize
    rescue NameError
      Logger.debug "rejecting #{model} for target"
      nil
    end

    def candidates
      @candidates = Array(source.models).map do |candidate|
        if candidate == "info:fedora/cm:Text.PDF"
          "TuftsPdf"
        elsif candidate == "info:fedora/cm:Text.TEI"
          "TuftsTei"
        elsif candidate == "info:fedora/cm:VotingRecord"
          "TuftsVotingRecord"
        elsif candidate == "info:fedora/cm:Text.EAD"
          "TuftsEad"
        elsif candidate == "info:fedora/cm:Image.4DS"
          "TuftsImage"
        elsif candidate == "info:fedora/cm:Audio"
          "TuftsAudio"
        elsif candidate == "info:fedora/cm:Audio.OralHistory"
          "TuftsAudio"
        elsif candidate == "info:fedora/afmodel:TuftsVideo"
          "TuftsVideo"
        elsif candidate == "info:fedora/cm:Text.RCR"
          "TuftsRcr"
        elsif candidate == "info:fedora/cm:Object.Generic"
          "TuftsGenericObject"
        else
          candidate
        end
      end
    end
  end


end

